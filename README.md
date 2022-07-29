# RocketMQ Clients - Collection of Client Bindings for Apache RocketMQ

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![CPP](https://github.com/apache/rocketmq-clients/actions/workflows/cpp_build.yml/badge.svg)](https://github.com/apache/rocketmq-clients/actions/workflows/cpp_build.yml)
[![C#](https://github.com/apache/rocketmq-clients/actions/workflows/csharp_build.yml/badge.svg)](https://github.com/apache/rocketmq-clients/actions/workflows/csharp_build.yml)
[![Java](https://github.com/apache/rocketmq-clients/actions/workflows/java_build.yml/badge.svg)](https://github.com/apache/rocketmq-clients/actions/workflows/java_build.yml)
[![Golang](https://github.com/apache/rocketmq-clients/actions/workflows/golang_build.yml/badge.svg)](https://github.com/apache/rocketmq-clients/actions/workflows/golang_build.yml)

## Overview

Client bindings for [Apache RocketMQ](https://rocketmq.apache.org/), all of them follow the specification of [rocketmq-apis](https://github.com/apache/rocketmq-apis), replacing 4.x remoting-based counterparts. Clients in this repository are built on top of [Protocol Buffers](https://developers.google.com/protocol-buffers) and [gRPC](https://grpc.io/).

## Goal

Provide cloud-native and robust solutions for Java, C++, C#, Golang, Rust and all other mainstream programming languages.

## Features and Status

|                    Feature                     | Java  | C/C++ |  C#   | Golang | Rust  | node.js | Python |
| :--------------------------------------------: | :---: | :---: | :---: | :----: | :---: | :-----: | :----: |
|        Producer with standard messages         |   ✅   |   ✅   |   ✅   |  WIP   |  WIP  |   WIP   |  WIP   |
|          Producer with FIFO messages           |   ✅   |   ✅   |   ✅   |  WIP   |  WIP  |   WIP   |  WIP   |
|          Producer with timed messages          |   ✅   |   ✅   |   ✅   |  WIP   |  WIP  |   WIP   |  WIP   |
|      Producer with transactional messages      |   ✅   |   ✅   |   ✅   |  WIP   |  WIP  |   WIP   |  WIP   |
|                Simple Consumer                 |   ✅   |   ✅   |   ✅   |  WIP   |  WIP  |   WIP   |  WIP   |
| Push Consumer with concurrent message listener |   ✅   |   ✅   |   ❌   |  WIP   |  WIP  |   WIP   |  WIP   |
|    Push Consumer with FIFO message listener    |   ✅   |   ✅   |   ❌   |  WIP   |  WIP  |   WIP   |  WIP   |

## Related

* [rocketmq](https://github.com/apache/rocketmq): The implementation of server-side.
* [rocketmq-apis](https://github.com/apache/rocketmq-apis): Common communication protocol between server and client.
* [RIP-37: New and Unified APIs](https://shimo.im/docs/m5kv92OeRRU8olqX): RocketMQ proposal of new and unified APIs crossing different languages.
* [RIP-39: Support gRPC protocol](https://shimo.im/docs/gXqmeEPYgdUw5bqo): RocketMQ proposal of gRPC protocol support.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation
