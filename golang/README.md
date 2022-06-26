Introduction
--------------
Project github.com/apache/rocketmq-clients/golang is targeted to implement with golang. At the current moment, it is still a work-in-progress project. Do not use it in production till it grows mature enough.

Architecture
--------------
Basically, this project would follow the same paradigm of [rocketmq-client-cpp v5.0.0](https://github.com/apache/rocketmq-client-cpp/tree/main). Namely, we would build the whole client following protocols described in [rocketmq-apis](https://github.com/apache/rocketmq-apis) on top of [gRPC-go](https://github.com/grpc/grpc-go), utilizing [Protocol buffers](https://developers.google.com/protocol-buffers) to serialize and deserialize data in transmission.


How to use
-----------------

#### Installation

With [Go module][] support (Go 1.11+), simply add the following import

```go
import "github.com/apache/rocketmq-clients/golang"
```

to your code, and then `go [build|run|test]` will automatically fetch the
necessary dependencies.

Otherwise, to install the `golang` package, run the following command:

```console
$ go get -u github.com/apache/rocketmq-clients/golang
```

License
------------------
This project follows [Apache License Version 2.0](./LICENSE).

How to contribute
------------------
Similar to other Apache RocketMQ projects, we welcome contributions in various ways, from filing a bug report, correcting type error, document writing to complete feature implementation. Any attempt to make this project better is welcome.

If this project catches your attention, do not hesitate to make a pull request.