package main

import (
	"context"
	"fmt"
	"github.com/yurizf/go-aws-msg-with-batching/batching"
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
		batching.WG.Add(1)
		go func() {
			defer batching.WG.Done()
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
					n, err := w.Write([]byte(msg))
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to write %d bytes into the msg writer: %s", len(msg), err))
						return
					}
					slog.Debug(fmt.Sprintf("Wrote %d:%d bytes to the buffer %p", len(msg), n, w))
					err = w.Close()
					if err != nil {
						slog.Error(fmt.Sprintf("Failed to close %d bytes msg writer buffer %p: %s", len(msg), w, err))
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
	time.Sleep(60 * time.Second)
	close(ch)

	batching.WG.Wait()

	fmt.Println(fmt.Sprintf("length of debug array is %d", len(batching.Debug.Debug)))
	for _, v := range batching.Debug.Debug {
		fmt.Println(v)
	}
	fmt.Println("Finishing...")
}