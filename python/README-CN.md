# Python 客户端

[English](README.md) | 简体中文 | [RocketMQ 官网](https://rocketmq.apache.org/)

## 概述

不同于[基于 RemotingCommand 协议的版本](https://github.com/apache/rocketmq/tree/develop/client)，当前的客户端基于 RocektMQ 5.0 存算分离架构进行设计开发，是 RocketMQ 社区目前推荐的接入方式，也是未来客户端演进的主要方向。

在开始客户端的部分之前，所需的一些前期工作（或者参照[这里](https://rocketmq.apache.org/zh/docs/quickStart/02quickstart/)）：

1. 准备 Python 环境。Python 3.5 是确保客户端运行的最小版本，Python 3.10 是推荐版本；
2. 部署 namesrv，broker 以及 [proxy](https://github.com/apache/rocketmq/tree/develop/proxy) 组件。

## 快速开始

在`rocketmq-clients/python`目录下使用命令：`pip install -r requirements.txt` 即可安装相关依赖库。

## 目前进展

* 协议层代码生成完毕
* rpcClient完成部分