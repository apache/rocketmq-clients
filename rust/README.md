# The Rust Implementation of Apache RocketMQ Client

[RocketMQ Website](https://rocketmq.apache.org/)

## Overview

Here is the rust implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/). Different from the [remoting-based client](https://github.com/apache/rocketmq/tree/develop/client), the current implementation is based on separating architecture for computing and storage, which is the more recommended way to access the RocketMQ service.

Here are some preparations you may need to know (or refer to [here](https://rocketmq.apache.org/docs/quickStart/02quickstart)).

## Getting Started

### Requirements

1. rust and cargo
2. protoc 3.15.0+
3. setup name server, broker, and [proxy](https://github.com/apache/rocketmq/tree/develop/proxy).

### Run Tests

```sh
cargo llvm-cov --ignore-filename-regex pb/ --open
```

### Run Example

Run the following command to start the example:

```sh
# send message via producer
cargo run --example producer

# consume message via simple consumer
cargo run --example simple_consumer
```

