module github.com/yurizf/go-aws-msg-with-batching/test/client

go 1.21

// require github.com/yurizf/go-aws-msg-with-batching

require github.com/yurizf/go-aws-msg-with-batching v1.0.0
replace github.com/yurizf/go-aws-msg-with-batching v1.0.0 => ../..

require (
	github.com/aws/aws-sdk-go v1.55.5 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/zerofox-oss/go-msg v0.1.4 // indirect
)
