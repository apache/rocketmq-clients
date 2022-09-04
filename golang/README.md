# The Golang Implementation of Apache RocketMQ Client

Project github.com/apache/rocketmq-clients/golang is targeted to implement with golang. At the current moment, it is still a work-in-progress project. Do not use it in production till it grows mature enough.

## Architecture

We build the following protocols described in [rocketmq-apis](https://github.com/apache/rocketmq-apis) on top of [gRPC-go](https://github.com/grpc/grpc-go), utilizing [Protocol buffers](https://developers.google.com/protocol-buffers) to serialize and deserialize data in transmission.

## Quick Start

### Installation

With [Go modules](https://go.dev/doc/go1.11#modules)(Go 1.11+), simply add the following import to your code, and then `go [build|run|test]` will automatically fetch the necessary dependencies.

```go
import "github.com/apache/rocketmq-clients/golang"
```

Otherwise, to install the `golang` package, run the following command:

```sh
go get -u github.com/apache/rocketmq-clients/golang
```
