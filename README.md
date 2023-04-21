# RocketMQ Clients - Collection of Client Bindings for Apache RocketMQ

[![License][license-image]][license-url]

[![CPP][cpp-image]][cpp-url] [![Codecov-cpp][codecov-cpp-image]][codecov-url]

[![C#][csharp-image]][csharp-url] [![NuGet Gallery][nuget-image]][nuget-url]

[![Java][java-image]][java-url] [![Codecov-java][codecov-java-image]][codecov-url] [![Maven Central][maven-image]][maven-url]

[![Golang][golang-image]][golang-url] [![Codecov-golang][codecov-golang-image]][codecov-url]

[![Rust][rust-image]][rust-url] [![Codecov-rust][codecov-rust-image]][codecov-url] [![Crates.io][crates-image]][crates-url] [![docs.rs][rust-doc-image]][rust-doc-url]

English | [简体中文](README-CN.md) | [RocketMQ Website](https://rocketmq.apache.org/)

## Overview

Client bindings for [Apache RocketMQ](https://rocketmq.apache.org/), as known as RocketMQ 5.x SDK. All of them follow the specification of [rocketmq-apis](https://github.com/apache/rocketmq-apis), replacing 4.x remoting-based counterparts. Clients in this repository are built on top of [Protocol Buffers](https://developers.google.com/protocol-buffers) and [gRPC](https://grpc.io/).

## Goal

Provide cloud-native and robust solutions for Java, C++, C#, Golang, Rust and all other mainstream programming languages.

## Features and Status

* Ready - ✅
* Work in progress - 🚧

| Feature                                        | Java  | C/C++ |  C#   | Golang | Rust  | Node.js | Python |
| ---------------------------------------------- | :---: | :---: | :---: | :----: | :---: | :-----: | :----: |
| Producer with standard messages                |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |    🚧    |   🚧    |
| Producer with FIFO messages                    |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |    🚧    |   🚧    |
| Producer with timed/delay messages             |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |    🚧    |   🚧    |
| Producer with transactional messages           |   ✅   |   ✅   |   ✅   |   ✅    |   🚧   |    🚧    |   🚧    |
| Simple consumer                                |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |    🚧    |   🚧    |
| Push consumer with concurrent message listener |   ✅   |   ✅   |   🚧   |   🚧    |   🚧   |    🚧    |   🚧    |
| Push consumer with FIFO message listener       |   ✅   |   ✅   |   🚧   |   🚧    |   🚧   |    🚧    |   🚧    |

## Prerequisite and Build

As this project is structured as a monorepo, instructions on how to build it can be found in the subdirectories for each language's bindings. Since the [rocketmq-apis](https://github.com/apache/rocketmq-apis) submodule is included in this project and may be referenced by some bindings, we strongly recommend that you clone this repository using the following command:

```sh
git clone --recursive git@github.com:apache/rocketmq-clients.git
```

## Contributing

Similar to other projects of Apache RocketMQ, any attempt to make this project better is welcome, including but not limited to filing a bug report, correcting type error or document writing to complete feature implementation. Do not hesitate to make a pull request if this project catches your attention.

## Related

* [rocketmq](https://github.com/apache/rocketmq): The implementation of server-side.
* [rocketmq-apis](https://github.com/apache/rocketmq-apis): Common communication protocol between server and client.
* [RIP-37: New and Unified APIs](https://shimo.im/docs/m5kv92OeRRU8olqX): RocketMQ proposal of new and unified APIs crossing different languages.
* [RIP-39: Support gRPC protocol](https://shimo.im/docs/gXqmeEPYgdUw5bqo): RocketMQ proposal of gRPC protocol support.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation

[license-image]: https://img.shields.io/badge/license-Apache%202-4EB1BA.svg
[license-url]: https://www.apache.org/licenses/LICENSE-2.0.html
[cpp-image]: https://github.com/apache/rocketmq-clients/actions/workflows/cpp_build.yml/badge.svg
[cpp-url]: https://github.com/apache/rocketmq-clients/actions/workflows/cpp_build.yml
[csharp-image]: https://github.com/apache/rocketmq-clients/actions/workflows/csharp_build.yml/badge.svg
[csharp-url]: https://github.com/apache/rocketmq-clients/actions/workflows/csharp_build.yml
[java-image]: https://github.com/apache/rocketmq-clients/actions/workflows/java_build.yml/badge.svg
[java-url]: https://github.com/apache/rocketmq-clients/actions/workflows/java_build.yml
[golang-image]: https://github.com/apache/rocketmq-clients/actions/workflows/golang_build.yml/badge.svg
[rust-image]: https://github.com/apache/rocketmq-clients/actions/workflows/rust_build.yml/badge.svg
[golang-url]: https://github.com/apache/rocketmq-clients/actions/workflows/golang_build.yml
[rust-url]: https://github.com/apache/rocketmq-clients/actions/workflows/rust_build.yml
[codecov-cpp-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=cpp&label=CPP%20Coverage&logo=codecov
[codecov-java-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=java&label=Java%20Coverage&logo=codecov
[codecov-golang-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=golang&label=Golang%20Coverage&logo=codecov
[codecov-rust-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=rust&label=Rust%20Coverage&logo=codecov
[codecov-url]: https://app.codecov.io/gh/apache/rocketmq-clients
[maven-image]: https://img.shields.io/maven-central/v/org.apache.rocketmq/rocketmq-client-java
[maven-url]: https://maven-badges.herokuapp.com/maven-central/org.apache.rocketmq/rocketmq-client-java
[nuget-image]: https://img.shields.io/nuget/v/RocketMQ.Client.svg?label=nuget
[nuget-url]: https://www.nuget.org/packages/RocketMQ.Client/
[crates-image]: https://img.shields.io/crates/v/rocketmq.svg
[crates-url]: https://crates.io/crates/rocketmq
[rust-doc-image]: https://docs.rs/rocketmq/badge.svg
[rust-doc-url]: https://img.shields.io/crates/v/rocketmq.svg
