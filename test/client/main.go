package main

import (
	"context"
	"fmt"
	"github.com/yurizf/go-aws-msg-with-batching/sns"
	"log/slog"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const allRunesString = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_-/+?!@#$%^&*()[]世界象形字形声嶝 (dèng; mountain path)=山(shān;mountain)+登(dēng)"

var allRunes []rune = []rune(allRunesString)

func randString(minLen int, maxLen int) string {
	// inclusive
	n := rand.Intn(maxLen-minLen+1) + minLen
	b := make([]rune, n)
	for i := range b {
		b[i] = allRunes[rand.Int63()%int64(len(allRunes))]
	}
	// Encode Unicode code points as UTF-8
	// Invalid code points converted to Unicode replacement character (U+FFFD). Should not be any
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
		config.numberOfGoRoutines, _ = strconv.Atoi(r)
	}

	if r := os.Getenv("TOPIC_ARN"); r != "" {
		config.topic_arn = r
	}

	fmt.Println("Configuration", config)

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
	var wg sync.WaitGroup
	for i := 0; i < config.numberOfGoRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
						slog.Info("Channel is closed. Shutting down the topic...")
						ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
						defer cancel()
						if err := shutdownFunc(ctx); err != nil {
							slog.Error(fmt.Sprintf("error calling shutdown func: %s", err))
						}
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
		msg := randString(100, 15000) // message is uuencoded and gets longer
		slog.Debug(fmt.Sprintf("Generated random string of %d bytes", len(msg)))
		ch <- msg
	}

	ch <- "POISON_PILL"
	time.Sleep(60 * time.Second)

	fmt.Println("closing the message feeding chan.....")
	close(ch)
	wg.Wait()

	fmt.Printf("after closing channel and waiting for 30 secs, the number of go routines should be zero. it is %d\n\n", runtime.NumGoroutine())

}
