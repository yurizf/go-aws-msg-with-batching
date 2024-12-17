package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yurizf/go-aws-msg-with-batching/sns"
	"log"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

const allRunesString = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890_-/+?!@#$%^&*()[]世界象形字形声嶝 (dèng; mountain path)=山(shān;mountain)+登(dēng)"

var allRunes []rune = []rune(allRunesString)

func MD5(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

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
	db_url             string
}{1000, 10, "", ""}

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

	if r := os.Getenv("DB_URL"); r != "" {
		config.db_url = r
	}
	fmt.Println("Configuration", config)

	ch := make(chan string)
	var wg sync.WaitGroup
	for i := 0; i < config.numberOfGoRoutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			topic, uuid, shutdownFunc, err := sns.NewBatchedTopic(config.topic_arn, true)
			if err != nil {
				log.Printf("[ERROR]  creating topic %s: %s", config.topic_arn, err)
				return
			}

			ctx := context.Background()
			i := 0
			for {
				i = i + 1
				select {
				case msg, ok := <-ch:
					if !ok {
						log.Printf("[INFO] in main: Channel is closed. Shutting down the topic " + uuid)
						ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
						defer cancel()
						if err := shutdownFunc(ctx); err != nil {
							log.Printf("[ERROR] calling shutdown func: %s", err)
						}
						return // channel closed
					}

					w := topic.NewWriter(ctx)
					if msg == "POISON_PILL" {
						log.Printf("[INFO] writing POISON_PILL into a topic")
					}
					w.Attributes().Set("count", fmt.Sprintf("%d", i))
					_, err := w.Write([]byte(msg))
					if err != nil {
						log.Printf("[ERROR] Failed to write %d bytes into the msg writer: %s", len(msg), err)
						return
					}
					err = w.Close()
					if err != nil {
						log.Printf("[ERROR] Failed to close %d bytes msg writer buffer %p: %s", len(msg), w, err)
						return
					}
					//if _, err := io.Copy(w, m.Body); err != nil {
				default:
				}
			}

		}()
	}

	dbCtx := context.Background()
	pgConn, err := pgxpool.New(dbCtx, config.db_url)
	if err != nil {
		log.Printf("[ERROR] failed to connect to %s: %s", config.db_url, err)
		return
	}
	defer pgConn.Close()

	insertSQL := "INSERT INTO client (l, md5, msg) VALUES ($1,$2, $3)"

	for i := 0; i < config.numberOfMessages; i = i + 1 {
		msg := randString(100, 15000) // message is uuencoded and gets longer
		msg = fmt.Sprintf("msg %d:%s", i, msg)
		log.Printf("[TRACE] generated msg of %d bytes: %s", len(msg), msg[:20])
		ch <- msg
		_, err := pgConn.Exec(dbCtx, insertSQL, len(msg), MD5(msg), msg)
		if err != nil {
			log.Printf("[ERROR] writing %d bytes to the database: %s", len(msg), err)
		}
	}

	ch <- "POISON_PILL"
	_, err = pgConn.Exec(dbCtx, insertSQL, len("POISON_PILL"), MD5("POISON_PILL"), "POISON_PILL")

	// this should be more than enough to send all the batched messages on timeout
	time.Sleep(60 * time.Second)

	fmt.Println("closing the message feeding chan.....")
	close(ch)
	wg.Wait()

	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	fmt.Printf("after closing channel and waiting for 30 secs, the number of go routines should be zero. it is: %d\n", runtime.NumGoroutine())
	fmt.Println(string(buf[:n]))
}
