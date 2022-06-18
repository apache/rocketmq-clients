# RocketMQ Clients - Collection of Polyglot Clients for Apache RocketMQ

[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)
[![CPP](https://github.com/apache/rocketmq-clients/actions/workflows/cpp_build.yml/badge.svg)](https://github.com/apache/rocketmq-clients/actions/workflows/cpp_build.yml)
[![C#](https://github.com/apache/rocketmq-clients/actions/workflows/csharp_build.yml/badge.svg)](https://github.com/apache/rocketmq-clients/actions/workflows/csharp_build.yml)
[![Java](https://github.com/apache/rocketmq-clients/actions/workflows/java_build.yml/badge.svg)](https://github.com/apache/rocketmq-clients/actions/workflows/java_build.yml)

## Overview

Polyglot solution of clients for [Apache RocketMQ](https://rocketmq.apache.org/), and both of them follow the specification of [rocketmq-apis](https://github.com/apache/rocketmq-apis), replace the previous protocol based on `RemotingCommand` by [Protocol Buffers](https://developers.google.com/protocol-buffers) and [gRPC](https://grpc.io/). Apart from that, APIs from clients of different languages share the same model and semantics.

## Goal

Provides cloud-native and robust solution for mainstream programming languages.

## Related

* [rocketmq](https://github.com/apache/rocketmq): The implementation of server-side.
* [rocketmq-apis](https://github.com/apache/rocketmq-apis): Common communication protocol between server and client.
* [RIP-37: New and Unified APIs](https://shimo.im/docs/m5kv92OeRRU8olqX): RocketMQ proposal of new and unified APIs acrossing different languages.
* [RIP-39: Support gRPC protocol](https://shimo.im/docs/gXqmeEPYgdUw5bqo): RocketMQ proposal of gRPC protocol support.

## License

[Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0.html) Copyright (C) Apache Software Foundation
