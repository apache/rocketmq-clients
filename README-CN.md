# Apache RocketMQ 客户端

[![License][license-image]][license-url] [![Build][build-image]][build-url]

[English](README.md) | 简体中文 | [RocketMQ 官网](https://rocketmq.apache.org/)

## 概述

[Apache RocketMQ](https://rocketmq.apache.org/) 的多语言客户端实现，也被称为 RocketMQ 5.x 客户端。遵从 [rocketmq-apis](https://github.com/apache/rocketmq-apis) 约束，使用 [Protocol Buffers](https://developers.google.com/protocol-buffers) 和 [gRPC](https://grpc.io/) 替代了 4.x 的旧有协议。

## 设计目标

为 Apache RocketMQ 提供包含 Java，C++，C#，Golang，JavaScript，Rust 在内的所有主流编程语言的云原生的，健壮的客户端解决方案。

## 特性与进度

* 可用 - ✅
* 进行中 - 🚧

| 特性                                             | Java  | C/C++  |  C#   | Golang | Rust | Python | Node.js |  PHP  |
|------------------------------------------------| :---: |:------:|:-----:|:------:|:----:|:------:|:-------:| :---: |
| Producer with standard messages                |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |   ✅    |    ✅    |   🚧   |
| Producer with FIFO messages                    |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |   ✅    |    ✅    |   🚧   |
| Producer with timed/delay messages             |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |   ✅    |    ✅    |   🚧   |
| Producer with transactional messages           |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |   ✅    |    ✅    |   🚧   |
| Producer with recalling timed/delay messages   |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |   ✅    |    ✅    |   🚧   |
| Simple consumer                                |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |   ✅    |    ✅    |   🚧   |
| Push consumer with concurrent message listener |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |   ✅    |    ✅    |   🚧   |
| Push consumer with FIFO message listener       |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |   ✅    |    ✅    |   🚧   |
| Push consumer with FIFO consume accelerator    |   ✅   |   ✅   |   ✅   |   ✅    |   ✅   |   ✅    |    ✅    |   🚧   |
| Priority Message                               |   ✅   |   🚧   |   ✅   |   ✅    |   ✅   |   ✅    |    ✅    |   🚧   |

## 先决条件和构建

本项目是以多语言 monorepo 的形式组织的，因此可以在每种语言的子目录中找到各自的构建命令。此外，由于 [rocketmq-apis](https://github.com/apache/rocketmq-apis) 作为一个子模块被包含在本项目中，可能会被一些语言的实现在构建时所引用，为了保证构建的顺利，我们强烈建议使用以下命令克隆此代码仓库：

```sh
git clone --recursive git@github.com:apache/rocketmq-clients.git
```

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
[build-image]: https://github.com/apache/rocketmq-clients/actions/workflows/build.yml/badge.svg
[build-url]: https://github.com/apache/rocketmq-clients/actions/workflows/build.yml
