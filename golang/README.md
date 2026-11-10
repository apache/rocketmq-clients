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

### Running Examples

The producer and consumer examples read their endpoint and authentication credentials from environment variables:

| Variable | Description |
| --- | --- |
| `ROCKETMQ_ENDPOINT` | RocketMQ endpoint in `host:port` format (required). |
| `ROCKETMQ_ACCESS_KEY` | Access key for broker authentication. |
| `ROCKETMQ_SECRET_KEY` | Secret key for broker authentication. |

Set `ROCKETMQ_ENDPOINT` before running an example. There is no default endpoint; an unset or empty value causes client creation to fail.

When broker authentication is enabled, also set both `ROCKETMQ_ACCESS_KEY` and `ROCKETMQ_SECRET_KEY` in your runtime environment. Keep real credentials out of source files and version control.

Unset credential variables are read as empty strings. For brokers that do not require authentication, these variables can remain unset.

Configure the topic and consumer group in the example as needed, then run it from the `golang` directory. For example, to connect to a local endpoint:

```sh
export ROCKETMQ_ENDPOINT='localhost:8081'
go run ./example/producer/normal
```

## Development

### Protocol Buffers Generation

If you need to regenerate the Protocol Buffers code after modifying the `.proto` files, run the following commands:

```bash
protoc --go-grpc_out=. apache/rocketmq/v2/*.proto
protoc --go_out=. apache/rocketmq/v2/*.proto
```

### Building

To build the entire project:

```bash
go build ./...
```

### Running Tests

To run all tests:

```bash
go test ./...
```

[codecov-golang-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=golang&label=Golang%20Coverage&logo=codecov
[codecov-url]: https://app.codecov.io/gh/apache/rocketmq-clients
