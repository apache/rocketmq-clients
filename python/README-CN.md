# Python 客户端

[English](README.md) | 简体中文 | [RocketMQ 官网](https://rocketmq.apache.org/)

## 概述

不同于[基于 RemotingCommand 协议的版本](https://github.com/apache/rocketmq/tree/develop/client)，当前的客户端基于 RocektMQ 5.0 存算分离架构进行设计开发，是 RocketMQ 社区目前推荐的接入方式，也是未来客户端演进的主要方向。

在开始客户端的部分之前，所需的一些前期工作（或者参照[这里](https://rocketmq.apache.org/zh/docs/quickStart/02quickstart/)）：

1. 准备 Python 环境。Python 3.7 是确保客户端运行的最小版本，Python 3.10 是推荐版本；
2. 部署 namesrv，broker 以及 [proxy](https://github.com/apache/rocketmq/tree/develop/proxy) 组件。

## 快速开始

推荐使用 Python 虚拟环境进行开发，可以按照以下步骤操作：

首先切换到当前仓库的 `python` 子目录，然后执行以下命令创建一个新的虚拟环境：

```sh
python3 -m venv myvenv
```

其次开始激活虚拟环境。激活方法取决于具体的操作系统：

* 对于Windows，执行：`myvenv\Scripts\activate.bat`
* 对于macOS/Linux：执行：`source myvenv/bin/activate`

执行以下命令以安装所需的依赖库：

```sh
pip install -r requirements.txt
```

## 目前进展

* 协议层代码生成完毕
* rpcClient完成部分
