package server

import (
	"context"
	"errors"
	"fmt"
	"github.com/yurizf/go-aws-msg-with-batching/sqs"
	"github.com/zerofox-oss/go-msg"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"
)

func main() {
	var topic_arn string
	if r := os.Getenv("TOPIC_ARN"); r != "" {
		topic_arn = r
	}

	sqsSrv, err := sqs.NewServer(topic_arn, 10, int64(30))
	if err != nil {
		slog.Error("Error creating Server: %s", err)
		return
	}

	stats := struct {
		mux            sync.Mutex
		numberMessages int
		totalLength    int64
	}{}

	receiverFunc := msg.ReceiverFunc(

		func(ctx context.Context, m *msg.Message) error {
			data, _ := io.ReadAll(m.Body)
			str := string(data)
			if str == "POISON_PILL" {
				cntx, cancel := context.WithTimeout(ctx, time.Duration(5*time.Second))
				defer cancel()

				sqsSrv.Shutdown(cntx) // we are done. Cancel the top context from the closure.
				return nil
			}

			stats.mux.Lock()
			defer stats.mux.Unlock()
			stats.numberMessages = stats.numberMessages + 1
			stats.totalLength = stats.totalLength + int64(len(str))
			return nil
		})

	err = sqsSrv.Serve(receiverFunc)
	if !errors.Is(err, msg.ErrServerClosed) {
		slog.Error("Server closed with an error: %s", err)
	}

	fmt.Printf("SQS read and processed Total unbatched messages: %d, total length: %d", stats.numberMessages, stats.totalLength)

}
