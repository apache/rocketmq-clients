# The Golang Implementation of Apache RocketMQ Client

[![Codecov-golang][codecov-golang-image]][codecov-url]

Here is the golang implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/).

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
go get -u github.com/apache/rocketmq-clients/golang/v5
```

[codecov-golang-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=golang&label=Golang%20Coverage&logo=codecov
[codecov-url]: https://app.codecov.io/gh/apache/rocketmq-clients
