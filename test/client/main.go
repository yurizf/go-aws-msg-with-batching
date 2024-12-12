package main

import (
	"context"
	"fmt"
	"github.com/yurizf/go-aws-msg-with-batching/sns"
	"log/slog"
	"math/rand"
	"os"
	"strconv"
	"time"
)

const allChars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_-/+?!@#$%^&*()[]"

func randString(minLen int, maxLen int) string {
	// inclusive
	n := rand.Intn(maxLen-minLen+1) + minLen
	b := make([]byte, n)
	for i := range b {
		b[i] = allChars[rand.Int63()%int64(len(allChars))]
	}
	return string(b)
}

var config = struct {
	numberOfMessages   int
	numberOfGoRoutines int
	topic_arn          string
}{1000, 10, ""}

func main() {
	if r := os.Getenv("TOTAL_MESSAGES"); r != "" {
		config.numberOfMessages, _ = strconv.Atoi(r)
	}

	if r := os.Getenv("CONCURRENCY"); r != "" {
		config.numberOfMessages, _ = strconv.Atoi(r)
	}

	if r := os.Getenv("TOPIC_ARN"); r != "" {
		config.topic_arn = r
	}

	// https://betterstack.com/community/guides/logging/logging-in-go/
	if r := os.Getenv("LOG_LEVEL"); r != "" {
		opts := &slog.HandlerOptions{
			Level: slog.LevelInfo,
		}
		logLevel := r
		switch logLevel {
		case "debug":
			opts = &slog.HandlerOptions{
				Level: slog.LevelDebug,
			}
		case "error":
			opts = &slog.HandlerOptions{
				Level: slog.LevelError,
			}
		case "warn":
			opts = &slog.HandlerOptions{
				Level: slog.LevelWarn,
			}
		}
		slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, opts)))
	}

	ch := make(chan string)
	for i := 0; i < config.numberOfGoRoutines; i = i + 1 {
		go func() {
			topic, shutdownFunc, err := sns.NewBatchedTopic(config.topic_arn)
			if err != nil {
				slog.Error("Error creating topic %s: %s", config.topic_arn, err)
				return
			}

			ctx := context.Background()
			i := 0
			for {
				i = i + 1
				select {
				case msg, ok := <-ch:
					if !ok {
						slog.Info("Channel is closed. Shutting down...")
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						shutdownFunc(ctx)
						return // channel closed
					}

					w := topic.NewWriter(ctx)
					w.Attributes().Set("count", fmt.Sprintf("%d", i))
					_, err := w.Write([]byte(msg))
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to write %d bytes into the msg writer: %s", len(msg), err))
						return
					}
					slog.Debug(fmt.Sprintf("Wrote %d bytes to the buffer", len(msg)))
					err = w.Close()
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to close %d bytes msg writer buffer: %s", len(msg), err))
						return
					}
					//if _, err := io.Copy(w, m.Body); err != nil {
				default:
				}
			}

		}()
	}

	for i := 0; i < config.numberOfMessages; i = i + 1 {
		msg := randString(100, 240000)
		slog.Debug(fmt.Sprintf("Generated random string of %d bytes", len(msg)))
		ch <- msg
	}

	ch <- "POISON_PILL"

	close(ch)

}
