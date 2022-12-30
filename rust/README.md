# The Rust Implementation of Apache RocketMQ Client

Here is the rust implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/).
It is still at a very early stage of development.

## Prerequisites

| Stage   | Requirements |
| ------- |--------------|
| Build   | Rust 1.66+   |

## Getting Started

Firstly, add the dependency to your `Cargo.toml`, and replace the `version` with the latest version.

```toml
[dependencies]
rocketmq = { version = "0.1", features = ["default"] }
```

Note: `features = ["default"]` contains `producer` and `consumer`, you can select only one of these features.

You can see more code examples [here](./examples).

### Usage of Producer
- TODO

### Usage of Consumer
- TODO

## Contribute
Ensure that the following command is executed and passed

- Build with `cargo build`

- Run `cargo clippy --all` - this will catch common mistakes and improve your Rust code.

- Run `cargo fmt --all` - this will find and fix code formatting issues.

- Run `cargo test --all-targets`
