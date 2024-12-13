package batching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/yurizf/go-aws-msg-with-batching/awsinterfaces"
	"log/slog"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const MAX_MSG_LENGTH int = 262144
const SNS = "sns"
const SQS = "sqs"

type msg struct {
	placed  time.Time
	payload string
}

func (m msg) byteLen() int {
	return batchLen([]string{m.payload})
}

// &{false {0 0} <nil> map[] map[] map[] map[] <nil>}
type Topic struct {
	queueType     string
	arnOrUrl      string
	mux           sync.Mutex
	timeout       time.Duration
	snsClient     awsinterfaces.SNSPublisher
	snsAttributes map[string]*sns.MessageAttributeValue
	sqsClient     awsinterfaces.SQSSender
	sqsAttributes map[string]*sqs.MessageAttributeValue

	batch    strings.Builder
	earliest time.Time
	number   int

	overflow []msg

	resend []string

	concurrency chan struct{}

	batcherCtx        context.Context    // context used to Topic the life of batcher engine
	batcherCancelFunc context.CancelFunc // CancelFunc for all receiver routines
}

var ctl Topic

// SetTopicTimeout - updates the timeout used to fire batched messages for a topic
// NewTopic should have been called for the topic prior to this call
func (ctl *Topic) SetTopicTimeout(timeout time.Duration) {
	ctl.timeout = timeout
}

// SetAttributes - sets a single attributes set for ALL queued msgs of a topic.
// NewTopic should have been called for the topic prior to this call
func (ctl *Topic) SetAttributes(attrs any) {

	ctl.mux.Lock()
	defer ctl.mux.Unlock()

	switch attrs.(type) {
	case map[string]*sns.MessageAttributeValue:
		ctl.snsAttributes = attrs.(map[string]*sns.MessageAttributeValue)
	case map[string]*sqs.MessageAttributeValue:
		ctl.sqsAttributes = attrs.(map[string]*sqs.MessageAttributeValue)
	}
}

// Append - batch analogue of "send". Adds the payload to the current batch
func (ctl *Topic) Append(payload string) error {
	if len(payload) > MAX_MSG_LENGTH {
		return fmt.Errorf("message is too long: %d", len(payload))
	}

	p := encode(payload)

	slog.Debug(fmt.Sprintf("appending %d to %d", len(p), ctl.batch.Len()))

	ctl.mux.Lock()
	defer ctl.mux.Unlock()
	if ctl.batch.Len()+len(p) > MAX_MSG_LENGTH {
		// slog.Debug(fmt.Sprintf("reached max SNS msg size, sending %d bytes", ctl.batch.Len()))

		ctl.overflow = append(ctl.overflow, msg{time.Now(), p})
		// don't send from here. It's cleaner to send from one place: engine
		return nil
	}

	if ctl.batch.Len() == 0 {
		ctl.earliest = time.Now()
	}

	_, err := ctl.batch.WriteString(p)

	return err
}

func (ctl *Topic) send(payload string) error {
	ctx, cancel := context.WithTimeout(ctl.batcherCtx, 500*time.Millisecond)
	defer cancel()

	var err error = nil
	switch ctl.queueType {
	case SNS:
		params := &sns.PublishInput{
			Message:  aws.String(payload),
			TopicArn: aws.String(ctl.arnOrUrl),
		}

		if len(ctl.snsAttributes) > 0 {
			params.MessageAttributes = ctl.snsAttributes
		}

		slog.Debug(fmt.Sprintf("in send func: sending message of %d bytes to sns %s", len(payload), ctl.arnOrUrl))
		Debug.Mux.Lock()
		Debug.Debug = append(Debug.Debug, fmt.Sprintf("%s: %d batchLen: %d resendLen: %d overflowLen: %d", time.Now(), getGOID(), len(payload), len(ctl.resend), len(ctl.overflow)))
		Debug.Mux.Unlock()

		for i := 0; i < 3; i++ {
			_, err = ctl.snsClient.PublishWithContext(ctx, params)
			if err != nil {
				slog.Error(fmt.Sprintf("error sending message of %d bytes to sns %s: %s", len(payload), ctl.arnOrUrl, err.Error()))
				time.Sleep(time.Duration(int64((i+1)*100) * int64(time.Millisecond)))
				continue
			}
			break
		}
	case SQS:
		params := &sqs.SendMessageInput{
			MessageBody: aws.String(payload),
			QueueUrl:    aws.String(ctl.arnOrUrl),
		}

		if len(ctl.sqsAttributes) > 0 {
			params.MessageAttributes = ctl.sqsAttributes
		}

		slog.Debug(fmt.Sprintf("sending message of %d bytes to sqs %s", len(payload), ctl.arnOrUrl))
		for i := 0; i < 3; i++ {
			_, err = ctl.sqsClient.SendMessageWithContext(ctx, params)
			if err != nil {
				slog.Error(fmt.Sprintf("error sending message of %d bytes to sqs %s: %s", len(payload), ctl.arnOrUrl, err.Error()))
				time.Sleep(time.Duration(int64((i+1)*100) * int64(time.Millisecond)))
				continue
			}
			break
		}
	}

	return err
}

// NewTopic creates and initializes the batching the engine data structures for a specific c sns/sqs.Topic
//
// It accepts the topic ARN,
// an SNSPublisher or SQSSender interface instance (implemented as AWS SNS or SQS clients).
// and the timeout value for this topic: upon its expiration the batch will be sendMessages to the topic
// generics with unions referencing interfaces with methods are not currently supported. Hence, any and type assertions.
// https://github.com/golang/go/issues/45346#issuecomment-862505803
func NewTopic(topicARN string, p any, timeout time.Duration, concurrency ...int) (*Topic, error) {

	topic := Topic{
		timeout:  timeout,
		arnOrUrl: topicARN,

		overflow: make([]msg, 0, 128),
		resend:   make([]string, 0, 128),
	}

	if len(concurrency) == 0 {
		topic.concurrency = make(chan struct{}, 10)
	} else {
		topic.concurrency = make(chan struct{}, concurrency[0])
	}

	switch v := p.(type) {
	case awsinterfaces.SNSPublisher:
		topic.snsClient = v
		topic.snsAttributes = make(map[string]*sns.MessageAttributeValue)
	case awsinterfaces.SQSSender:
		topic.sqsClient = v
		topic.sqsAttributes = make(map[string]*sqs.MessageAttributeValue)
	default:
		return nil, errors.New("Invalid client of unexpected type passed")
	}

	topic.batcherCtx, topic.batcherCancelFunc = context.WithCancel(context.Background())

	// this go routine is the sending engine for this topic
	// So, if the topic is used by multiple threads, only one instance of this routine runs.
	// if each thread created own topic for this arn/url, they won't collide.
	// either way it works
	WG.Add(1)
	go func() {
		defer WG.Done()
		for {
			select {
			case <-topic.batcherCtx.Done():
				slog.Info("batcher is shutting down...")

				close(topic.concurrency)
				return

			case <-time.After(100 * time.Millisecond):

				// first, resend failed on send msgs
				tmp := make([]string, 0, 128)
				for _, v := range topic.resend {
					// concurrency limit how many threads will hit SNS/SQS endpoint simultaneously
					topic.concurrency <- struct{}{}

					WG.Add(1)
					go func(s string) {
						defer func() {
							<-topic.concurrency
						}()
						defer WG.Done()

						slog.Debug(fmt.Sprintf("resending failed %d bytes to %s", len(s), topic.arnOrUrl))
						if err := topic.send(s); err != nil {
							topic.mux.Lock()
							tmp = append(tmp, s)
							topic.mux.Unlock()
						}
					}(v)
				}
				topic.resend = tmp

				if topic.batch.Len() > 0 && time.Now().Sub(topic.earliest) > topic.timeout {
					topic.mux.Lock()
					s := topic.batch.String()
					topic.batch.Reset()
					topic.mux.Unlock()

					if len(s) > 0 { // sanity check:
						topic.concurrency <- struct{}{}

						slog.Debug(fmt.Sprintf("sending a Batch of %d on timeout to topic %s", len(s), topic.arnOrUrl))

						// make it a go routine to unblock top level select
						// even tho we spawn only one go routine, we limit concurrency b/c we are in the loop
						WG.Add(1)
						go func(payload string) {
							defer func() {
								<-topic.concurrency
							}()
							defer WG.Done()

							err := topic.send(payload)

							topic.mux.Lock()
							defer topic.mux.Unlock()

							if err != nil {
								topic.resend = append(topic.resend, s)
							}

							if len(topic.overflow) > 0 {
								topic.earliest = topic.overflow[0].placed

								j := 0
								for i, o := range topic.overflow {
									j = i
									topic.batch.Write([]byte(encode(o.payload)))
									if i < len(topic.overflow)-1 && topic.batch.Len()+len(encode(topic.overflow[i+1].payload)) > MAX_MSG_LENGTH {
										break
									}
								}
								if j < len(topic.overflow)-1 {
									copy(topic.overflow[0:], topic.overflow[j+1:])
									topic.overflow = topic.overflow[:len(topic.overflow)-j]
								} else {
									topic.overflow = make([]msg, 0, 128)
								}
							}
						}(s)
					}
				}
			}
		}
	}()

	return &topic, nil
}

// Shutdown stops the batching engine and stops its go routine
// by calling cancel on the batcher context.
// It expects a context with a timeout to be passed to delay the shutdown
// so that all already accumulated messages could be sent.
func (ctl *Topic) ShutDown(ctx context.Context) error {
	if ctx == nil {
		panic("context not set in shutdown batcher")
	}
	slog.Info("shutting down batcher...")

	for {
		select {
		case <-ctx.Done():
			ctl.batcherCancelFunc()
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()

			return ctx.Err()
		default:
			time.Sleep(1 * time.Second)
		}
	}
}

var WG sync.WaitGroup

var Debug = struct {
	Mux   sync.Mutex
	Debug []string
}{
	Debug: make([]string, 0, 10000),
}

func ExtractGID(s []byte) int64 {
	s = s[len("goroutine "):]
	s = s[:bytes.IndexByte(s, ' ')]
	gid, _ := strconv.ParseInt(string(s), 10, 64)
	return gid
}

// Parse the goid from runtime.Stack() output. Slow, but it works.
func getGOID() int64 {
	var buf [64]byte
	return ExtractGID(buf[:runtime.Stack(buf[:], false)])
}
