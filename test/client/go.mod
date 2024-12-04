module github.com/yurizf/go-aws-msg-with-batching/test/client

go 1.21

// require github.com/yurizf/go-aws-msg-with-batching

require github.com/yurizf/go-aws-msg-with-batching v1.0.0

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)

replace github.com/yurizf/go-aws-msg-with-batching v1.0.0 => ../..

require (
	github.com/aws/aws-sdk-go v1.55.5 // indirect
	github.com/jackc/pgx/v5 v5.5.5
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/zerofox-oss/go-msg v0.1.4 // indirect
)
