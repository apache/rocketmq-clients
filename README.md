## Introduction

The current repository is a thin SDK for rocketmq based on [gRPC](https://grpc.io/), which replaces the communication
layer of the fat SDK.

## Requirements

1. Runtime: JRE>=6
2. Build: JDK>=11

## Build

The latest thin SDK support Java6 or higher. In order to achieve this goal, we customize gRPC and Protocol Buffer, it
could be referred below:

* [Customized gRPC](http://gitlab.alibaba-inc.com/rocketmq-client/grpc-java)
* [Customized Protocol Buffer](http://gitlab.alibaba-inc.com/rocketmq-client/protobuf)
* [Customized openTelemetry](http://gitlab.alibaba-inc.com/rocketmq-client/opentelemetry-java)

We have deployed all customized third-party jars to inner maven repository of Alibaba group, so you can compile the
project by executing the script below:

```bash
mvn clean package -DskipTests
```

## CI/CD
