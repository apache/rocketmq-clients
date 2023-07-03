# The Rust Implementation of Apache RocketMQ Client

[![Codecov-rust][codecov-rust-image]][codecov-url]
[![Crates.io][crates-image]][crates-url]
[![docs.rs][rust-doc-image]][rust-doc-url]

[RocketMQ Website](https://rocketmq.apache.org/)

## Overview

Here is the rust implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/). Different from the [remoting-based client](https://github.com/apache/rocketmq/tree/develop/client), the current implementation is based on separating architecture for computing and storage, which is the more recommended way to access the RocketMQ service.

Here are some preparations you may need to know [Quick Start](https://rocketmq.apache.org/docs/quickStart/02quickstart).

## Getting Started

### Requirements

1. rust toolchain, rocketmq's MSRV is 1.61.
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

[codecov-rust-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=rust&label=Rust%20Coverage&logo=codecov
[codecov-url]: https://app.codecov.io/gh/apache/rocketmq-clients
[crates-image]: https://img.shields.io/crates/v/rocketmq.svg
[crates-url]: https://crates.io/crates/rocketmq
[rust-doc-image]: https://img.shields.io/docsrs/rocketmq
[rust-doc-url]: https://docs.rs/rocketmq
