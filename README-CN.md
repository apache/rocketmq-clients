# Apache RocketMQ 客户端

[![License][license-image]][license-url]
[![CPP][cpp-image]][cpp-url]
[![C#][csharp-image]][csharp-url]
[![Java][java-image]][java-url]
[![Golang][golang-image]][golang-url]
[![PHP][php-image]][php-url]
[![Codecov-cpp][codecov-cpp-image]][codecov-url]
[![Codecov-java][codecov-java-image]][codecov-url]
[![Codecov-golang][codecov-golang-image]][codecov-url]
[![Maven Central][maven-image]][maven-url]

[English](README.md) | 简体中文 | [RocketMQ 官网](https://rocketmq.apache.org/)

## 概述

[Apache RocketMQ](https://rocketmq.apache.org/) 的多语言客户端实现。遵从 [rocketmq-apis](https://github.com/apache/rocketmq-apis) 约束，使用 [Protocol Buffers](https://developers.google.com/protocol-buffers) 和 [gRPC](https://grpc.io/) 替代了 4.x 的旧有协议。

## 设计目标

为 Apache RocketMQ 提供包含 Java，C++，C#，Golang，JavaScript，Rust 在内的所有主流编程语言的云原生的，健壮的客户端解决方案。

## 特性与进度

* 可用 - ✅
* 进行中 - 🚧

| 特性                                            | Java  | C/C++ |  C#   | Golang | Rust  | Node.js | Python |
| ---------------------------------------------- | :---: | :---: | :---: | :----: | :---: | :-----: | :----: |
| Producer with standard messages                |   ✅   |   ✅   |   ✅   |   ✅    |   🚧   |    🚧    |   🚧    |
| Producer with FIFO messages                    |   ✅   |   ✅   |   ✅   |   ✅    |   🚧   |    🚧    |   🚧    |
| Producer with timed/delay messages             |   ✅   |   ✅   |   ✅   |   ✅    |   🚧   |    🚧    |   🚧    |
| Producer with transactional messages           |   ✅   |   ✅   |   ✅   |   ✅    |   🚧   |    🚧    |   🚧    |
| Simple consumer                                |   ✅   |   ✅   |   ✅   |   ✅    |   🚧   |    🚧    |   🚧    |
| Push consumer with concurrent message listener |   ✅   |   ✅   |   🚧   |   🚧    |   🚧   |    🚧    |   🚧    |
| Push consumer with FIFO message listener       |   ✅   |   ✅   |   🚧   |   🚧    |   🚧   |    🚧    |   🚧    |

## 参与贡献

与 Apache RocketMQ 的其他项目类似，我们欢迎任何形式的贡献，包括但不仅限于提交 bug 报告、勘误纠错、文档撰写或提交 feature。成为 Apache RocketMQ contributor，从第一个 issue/pull request 开始！

## 相关链接

* [rocketmq](https://github.com/apache/rocketmq): RocketMQ 主仓库（含服务端实现）。
* [rocketmq-apis](https://github.com/apache/rocketmq-apis): RocketMQ 协议约束。
* [RIP-37: New and Unified APIs](https://shimo.im/docs/m5kv92OeRRU8olqX): RocketMQ 关于统一精简 API 设计的 RIP。
* [RIP-39: Support gRPC protocol](https://shimo.im/docs/gXqmeEPYgdUw5bqo): RocketMQ 关于 gRPC 协议支持的 RIP。

## 开源许可证

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
[golang-url]: https://github.com/apache/rocketmq-clients/actions/workflows/golang_build.yml
[php-image]: https://github.com/apache/rocketmq-clients/actions/workflows/php_build.yml/badge.svg
[php-url]: https://github.com/apache/rocketmq-clients/actions/workflows/php_build.yml
[codecov-cpp-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=cpp&label=CPP%20Coverage&logo=codecov
[codecov-java-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=java&label=Java%20Coverage&logo=codecov
[codecov-golang-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=golang&label=Golang%20Coverage&logo=codecov
[codecov-url]: https://codecov.io/gh/apache/rocketmq-clients/branch/master/
[maven-image]: https://maven-badges.herokuapp.com/maven-central/org.apache.rocketmq/rocketmq-client-java/badge.svg
[maven-url]: https://maven-badges.herokuapp.com/maven-central/org.apache.rocketmq/rocketmq-client-java
