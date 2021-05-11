## Introduction

The current repository is a thin SDK for rocketmq based on [gRPC](https://grpc.io/), which replaces the communication
layer of the fat SDK.

## Requirements

1. Runtime: JRE>=6
2. Build: JDK>=9 (**JDK11 is strongly recommended.**)

## Build

The latest thin SDK support Java6 or higher. In order to achieve this goal, we customize gRPC and Protocol Buffer, it
could be referred below:

* [Customized gRPC](http://gitlab.alibaba-inc.com/rocketmq-client/grpc-java)
* [Customized Protocol Buffer](http://gitlab.alibaba-inc.com/rocketmq-client/protobuf)

We recommend building the SDK using docker, which has installed the whole dependencies.

```bash
./ci/run_rocketmq_docker.sh 'mvn clean package'
```

Customized gRPC and Protocol Buffer should be installed ahead of time by executing the script below If you intend to
build using local environment:

```bash
./ci/install_deps.sh
```

## CI/CD
