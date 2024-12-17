package main

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/yurizf/go-aws-msg-with-batching/sqs"
	"github.com/zerofox-oss/go-msg"
	"io"
	"log"
	"os"
	"runtime"
	"sync"
	"time"
)

func MD5(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

func substr(s string) string {
	if len(s) > 20 {
		return s[:20]
	}
	return s
}

func main() {
	var topic_url string
	// https://betterstack.com/community/guides/logging/logging-in-go/
	if r := os.Getenv("TOPIC_URL"); r != "" {
		log.Printf("TOPIC_URL is %s", r)
		topic_url = r
	}

	var db_url string
	// https://betterstack.com/community/guides/logging/logging-in-go/
	if r := os.Getenv("DB_URL"); r != "" {
		log.Printf("DB_URL is %s", r)
		db_url = r
	}

	sqsSrv, err := sqs.NewBatchedServer(topic_url, 10, int64(30))
	if err != nil {
		log.Printf(fmt.Sprintf("[ERROR] creating Server: %s", err))
		return
	}

	stats := struct {
		mux            sync.Mutex
		numberMessages int
		totalLength    int64
	}{}

	dbCtx := context.Background()
	pgConn, err := pgxpool.New(dbCtx, db_url)
	if err != nil {
		log.Printf(fmt.Sprintf("[ERROR] connecting to %: %s", db_url, err))
		return
	}
	defer pgConn.Close()
	insertSQL := "INSERT INTO server (l, md5, msg) VALUES ($1, $2, $3)"

	receiverFunc := msg.ReceiverFunc(

		func(ctx context.Context, m *msg.Message) error {
			data, _ := io.ReadAll(m.Body)
			str := string(data)
			log.Printf("[TRACE] receiver obtained msg of %d bytes: %s", len(str), substr(str))
			_, err := pgConn.Exec(dbCtx, insertSQL, len(str), MD5(str), str)
			if err != nil {
				log.Printf("[ERROR] writing %d bytes to the database: %s", len(str), err)
			}

			if str == "POISON_PILL" {
				log.Printf("[TRACE] received POISON_PILL. Shutting down the server")
				// give it enough time to drain SQS queue
				cntx, cancel := context.WithTimeout(ctx, time.Duration(30*time.Second))
				defer cancel()

				sqsSrv.Shutdown(cntx) // we are done.
				return nil
			}

			stats.mux.Lock()
			defer stats.mux.Unlock()
			stats.numberMessages = stats.numberMessages + 1
			stats.totalLength = stats.totalLength + int64(len(str))
			return nil
		})

	log.Printf("Starting to serve...")
	err = sqsSrv.Serve(receiverFunc)
	if !errors.Is(err, msg.ErrServerClosed) {
		log.Printf("[ERROR] Server closed with an error: %s", err)
	}

	log.Printf("[TRACE] Server closed. Running diagnostics SQLs now...")

	sqls := []string{"SELECT COUNT(1) FROM client",
		"SELECT COUNT(1) FROM server",
		`SELECT client.l, client.md5
		 FROM client
		 EXCEPT    
		 SELECT server.l,server.md5
         FROM server`}

	var cnt int

	row := pgConn.QueryRow(dbCtx, sqls[0])
	switch err := row.Scan(&cnt); err {
	case nil:
		log.Printf("[TRACE] ****** Total number of client records from PG is %d *******\n", cnt)
	default:
		log.Printf("[ERROR] ****** error retrieveing Total number of client records from PG is %s *******\n", err)
	}

	row = pgConn.QueryRow(dbCtx, sqls[1])
	switch err := row.Scan(&cnt); err {
	case nil:
		log.Printf("[TRACE] ****** Total number of server records from PG is %d *******\n", cnt)
	default:
		log.Printf("[ERROR] ****** error retrieveing Total number of server records from PG is %s *******\n", err)
	}

	var l int
	var hash string

	rows, err := pgConn.Query(dbCtx, sqls[2])
	if err != nil {
		log.Printf("[ERROR] failed to execute sql %s: %s", sqls[2], err)
		return
	}
	defer rows.Close()
	log.Printf("[TRACE] ***** SQL Results: client messages that were not found in the server table")
	for rows.Next() {
		err := rows.Scan(&l, &hash)
		if err != nil {
			log.Printf("[ERROR] failed to scan in values: %s", err)
			return
		}
		log.Printf("[TRACE] %d %s", l, hash)
	}

	log.Printf("[TRACE] SQS read and processed Total unbatched messages: %d, total length: %d", stats.numberMessages, stats.totalLength)

	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, true)
	fmt.Printf("after closing channel and waiting for 30 secs, the number of go routines should be zero. it is: %d\n", runtime.NumGoroutine())
	fmt.Println(string(buf[:n]))

}
