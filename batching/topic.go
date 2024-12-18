package batching

import (
	"context"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/yurizf/go-aws-msg-with-batching/awsinterfaces"
	"log"
	"sync"
	"time"
)

const MAX_MSG_LENGTH int = 262144
const SNS = "sns"
const SQS = "sqs"
const SEND_TIMEOUT = 3 * time.Second

const ENCODING_ATTRIBUTE_KEY = "Content-Transfer-Encoding"
const ENCODING_ATTRIBUTE_VALUE = "partially-base64-batch"

// TODO: add a setter and option parsing for this value
var BATCH_TIMEOUT time.Duration = 2 * time.Second

type msg struct {
	placed  time.Time
	payload string
}

var id = struct {
	mux sync.Mutex
	id  int64
}{}

func ID() string {
	id.mux.Lock()
	defer id.mux.Unlock()
	id.id++
	return fmt.Sprintf("topic-%d", id.id)
}

type HighWaterMark struct {
	TimeStamp time.Time
	Number    int
	Length    int64
}

type Stats struct {
	Batch       HighWaterMark
	Overflow    HighWaterMark
	Resend      HighWaterMark
	TotalLength int64
	TotalMsg    int64
	Errors      int64
}

type Topic struct {
	queueType     string
	arnOrUrl      string
	ID            string
	mux           sync.Mutex
	timeout       time.Duration
	snsClient     awsinterfaces.SNSPublisher
	snsAttributes map[string]*sns.MessageAttributeValue
	sqsClient     awsinterfaces.SQSSender
	sqsAttributes map[string]*sqs.MessageAttributeValue

	batch       []msg
	batchString string

	overflow []msg

	//batcchStrings that failed to be sent
	resend []string

	concurrency chan struct{}

	batcherCtx        context.Context    // context used to Topic the life of batcher engine
	batcherCancelFunc context.CancelFunc // CancelFunc for all receiver routines

	wg sync.WaitGroup

	statsMux sync.Mutex
	stats    Stats
}

// SetTopicTimeout - updates the timeout used to fire batched messages for a topic
// NewTopic should have been called for the topic prior to this call
func (t *Topic) SetTopicTimeout(timeout time.Duration) {
	t.timeout = timeout
}

// SetAttributes - sets a single attributes set for ALL queued msgs of a topic.
// NewTopic should have been called for the topic prior to this call
func (t *Topic) SetAttributes(attrs any) {

	t.mux.Lock()
	defer t.mux.Unlock()

	switch attrs.(type) {
	case map[string]*sns.MessageAttributeValue:
		t.snsAttributes = attrs.(map[string]*sns.MessageAttributeValue)
	case map[string]*sqs.MessageAttributeValue:
		t.sqsAttributes = attrs.(map[string]*sqs.MessageAttributeValue)
	}
}

func (t *Topic) tryToAppend(m msg) bool {

	if 4+len(m.payload)+len(t.batchString) > MAX_MSG_LENGTH {
		return false
	}

	t.batch = append(t.batch, m)
	t.batchString = t.batchString + prefixWithLength(m.payload)
	return true
}

// Append - batch analogue of "send". Adds the payload to the current batch
func (t *Topic) Append(payload string) error {
	if len(payload) > MAX_MSG_LENGTH {
		return fmt.Errorf("message is too long: %d", len(payload))
	}

	Encode(payload)
	m := msg{time.Now(), Encode(payload)}
	log.Printf("[TRACE] %s: appending payload of %d bytes to %d", t.ID, len(payload), len(t.batch))

	t.mux.Lock()
	defer t.mux.Unlock()
	if !t.tryToAppend(m) {
		t.overflow = append(t.overflow, m)
	}

	// don't send from here. It's cleaner to send from one place: engine
	return nil
}

func (t *Topic) send(payload string) error {
	// from the tests, 500*time.Millisecond timout seems to be insufficient on messages 100K+ in size
	ctx, cancel := context.WithTimeout(t.batcherCtx, SEND_TIMEOUT)
	defer cancel()

	var err error = nil
	switch t.queueType {
	case SNS:
		params := &sns.PublishInput{
			Message:  aws.String(payload),
			TopicArn: aws.String(t.arnOrUrl),
		}

		if len(t.snsAttributes) > 0 {
			params.MessageAttributes = t.snsAttributes
		} else {
			// sanity check: this should never happen:
			return errors.New("expected content transfer attribute is missing")
		}

		log.Printf("[DEBUG] %s: in send func: sending message of %d bytes to sns %s", t.ID, len(payload), t.arnOrUrl)

		for i := 0; i < 3; i++ {
			_, err = t.snsClient.PublishWithContext(ctx, params)
			// debugging locally err := fmt.Errorf("fake")
			// fmt.Printf("*************** Fake SNS sending of %d bytes", len(*params.Message))
			// err = nil

			if err != nil {
				log.Printf("[ERROR] %s: error sending message of %d bytes with timeout %s to sns %s: %s", t.ID, len(payload), 3*time.Second, t.arnOrUrl, err.Error())
				t.stats.Errors++
				time.Sleep(time.Duration(int64((i+1)*100) * int64(time.Millisecond)))
				continue
			}
			break
		}
	case SQS:
		params := &sqs.SendMessageInput{
			MessageBody: aws.String(payload),
			QueueUrl:    aws.String(t.arnOrUrl),
		}

		if len(t.sqsAttributes) > 0 {
			params.MessageAttributes = t.sqsAttributes
		}

		log.Printf("[DEBUG] %s: sending message of %d bytes to sqs %s", t.ID, len(payload), t.arnOrUrl)
		for i := 0; i < 3; i++ {
			_, err = t.sqsClient.SendMessageWithContext(ctx, params)
			if err != nil {
				log.Printf("[ERROR] %s: error sending message of %d bytes to sqs %s: %s", t.ID, len(payload), t.arnOrUrl, err.Error())
				t.stats.Errors++
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

		batch:    make([]msg, 0, 128),
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
		topic.queueType = SNS
	case awsinterfaces.SQSSender:
		topic.sqsClient = v
		topic.sqsAttributes = make(map[string]*sqs.MessageAttributeValue)
		topic.queueType = SQS
	default:
		return nil, errors.New("Invalid client of unexpected type passed")
	}

	topic.batcherCtx, topic.batcherCancelFunc = context.WithCancel(context.Background())

	topic.ID = ID()

	// this go routine is the sending engine for this topic
	// So, if the topic is used by multiple threads, only one instance of this routine runs.
	// if each thread created own topic for this arn/url, they won't collide.
	// either way it works
	topic.wg.Add(1)
	go func() {
		defer topic.wg.Done()
		for {
			select {
			case <-topic.batcherCtx.Done():
				// by this time we should have nothing queued: deadlines should have taken care of it
				log.Printf("[INFO] %s topic's batching engine is shutting down. queued payload length is %d", topic.ID, len(topic.batch))

				close(topic.concurrency)
				return

			case <-time.After(100 * time.Millisecond):

				// first, resend failed on send msgs
				if len(topic.resend) > 0 {
					collectStats := false
					if len(topic.resend) > topic.stats.Resend.Number {
						topic.stats.Resend.Number = len(topic.resend)
						topic.stats.Resend.TimeStamp = time.Now()
						collectStats = true
					}

					tmp := make([]string, 0, 128)
					for _, v := range topic.resend {
						// concurrency limit how many threads will hit SNS/SQS endpoint simultaneously
						topic.concurrency <- struct{}{}
						if collectStats {
							topic.stats.Resend.Length = topic.stats.Resend.Length + int64(len(v))
						}

						topic.wg.Add(1)
						go func(s string) {
							defer func() {
								<-topic.concurrency
							}()
							defer topic.wg.Done()

							log.Printf("[DEBUG] %s: resending failed %d bytes to %s", topic.ID, len(s), topic.arnOrUrl)
							if err := topic.send(s); err != nil {
								topic.mux.Lock()
								tmp = append(tmp, s)
								topic.mux.Unlock()
							}
						}(v)
					}
					topic.resend = tmp
				}

				if len(topic.batch) > 0 && time.Now().Sub(topic.batch[0].placed) > topic.timeout {
					topic.mux.Lock()
					s := topic.batchString
					topic.stats.TotalMsg = topic.stats.TotalMsg + int64(len(topic.batch))
					topic.batch = make([]msg, 0, 128)
					topic.batchString = ""
					topic.stats.TotalLength = topic.stats.TotalLength + int64(len(s))

					topic.mux.Unlock()

					topic.concurrency <- struct{}{}
					// make it a go routine to unblock top level select
					// even tho we spawn only one go routine, we limit concurrency b/c we are in the loop
					topic.wg.Add(1)
					go func(payload string) {
						defer func() {
							<-topic.concurrency
						}()
						defer topic.wg.Done()

						err := topic.send(payload)

						topic.mux.Lock()
						defer topic.mux.Unlock()

						if err != nil {
							topic.resend = append(topic.resend, s)
						}

						if len(topic.overflow) > 0 {

							if len(topic.overflow) > topic.stats.Overflow.Number {
								topic.stats.Overflow.Number = len(topic.overflow)
								topic.stats.Overflow.TimeStamp = time.Now()
							}

							j := 0
							for i, o := range topic.overflow {
								if topic.tryToAppend(o) {
									j = i
									continue
								}
								break
							}
							log.Printf("[DEBUG] %s: copied %d overflow messages into batch", topic.ID, j)

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
	}()

	return &topic, nil
}

// Shutdown stops the batching engine and stops its go routine
// by calling cancel on the batcher context.
// It expects a context with a timeout to be passed to delay the shutdown
// so that all already accumulated messages could be sent.
func (t *Topic) ShutDown(ctx context.Context) error {
	if ctx == nil {
		panic("context not set in shutdown batcher")
	}
	deadline, ok := ctx.Deadline()
	if ok {
		log.Printf("[INFO] %s: shutting down topic's batcher...in %s", t.ID, deadline.Sub(time.Now()))
	} else {
		log.Printf("[INFO] %s: shutting down topic's batcher...", t.ID)
	}

	t.mux.Lock()
	log.Printf("[INFO] %s: Topic Statistics: %v", t.ID, t.stats)
	t.mux.Unlock()

	for {
		select {
		case <-ctx.Done():
			t.batcherCancelFunc()
			ticker := time.NewTicker(10 * time.Second)
			defer ticker.Stop()
			log.Printf("[INFO] %s: waiting for go routines to finish....", t.ID)
			t.wg.Wait()
			return ctx.Err()
		default:
			time.Sleep(1 * time.Second)
		}
	}
}
