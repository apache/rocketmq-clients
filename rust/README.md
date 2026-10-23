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

### Lite Topic

Lite topics are sub-topics of a parent topic with reduced metadata and storage overhead.

Server side preparation:

1. start the broker with `enableLmq=true` and `enableMultiDispatch=true`;
2. create the parent topic with `message.type=LITE`:

   ```sh
   sh mqadmin updateTopic -n <namesrv> -c <cluster> -t <parentTopic> -a +message.type=LITE
   ```

3. create the consumer group and bind it to the parent topic:

   ```sh
   sh mqadmin updateSubGroup -n <namesrv> -c <cluster> -g <group> --attributes "+lite.bind.topic=<parentTopic>"
   ```

4. run the proxy in CLUSTER mode, the LOCAL mode does not serve lite subscriptions.

Send a lite message:

```rust
let message = MessageBuilder::lite_message_builder("parentTopic", body, "lite-topic-1").build()?;
let receipt = producer.send(message).await?;
```

Pull and ack lite messages with `LiteSimpleConsumer`, which is bound to one parent topic and
subscribes lite topics dynamically:

```rust
let mut consumer = LiteSimpleConsumer::new(client_option, option, "parentTopic".to_string())?;
consumer.start().await?;
consumer
    .subscribe_lite_with_offset("lite-topic-1".to_string(), OffsetOption::from_policy(OffsetPolicy::Min))
    .await?;

let messages = consumer.receive(32, Duration::from_secs(15)).await?;
for message in &messages {
    println!("{}", message.message_id());
    consumer.ack(message).await?;
}

consumer.unsubscribe_lite("lite-topic-1".to_string()).await?;
consumer.shutdown().await?;
```

A runnable version lives in `examples/lite_simple_consumer.rs`:

```sh
cargo run --example lite_simple_consumer
```

[codecov-rust-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=rust&label=Rust%20Coverage&logo=codecov
[codecov-url]: https://app.codecov.io/gh/apache/rocketmq-clients
[crates-image]: https://img.shields.io/crates/v/rocketmq.svg
[crates-url]: https://crates.io/crates/rocketmq
[rust-doc-image]: https://img.shields.io/docsrs/rocketmq
[rust-doc-url]: https://docs.rs/rocketmq
