# Python 客户端

[English](README.md) | 简体中文 | [RocketMQ 官网](https://rocketmq.apache.org/)

## 概述

不同于[基于 RemotingCommand 协议的版本](https://github.com/apache/rocketmq/tree/develop/client)，当前的客户端基于 RocektMQ 5.0 存算分离架构进行设计开发，是 RocketMQ 社区目前推荐的接入方式，也是未来客户端演进的主要方向。

在开始客户端的部分之前，所需的一些前期工作（或者参照[这里](https://rocketmq.apache.org/zh/docs/quickStart/02quickstart/)）：

1. 准备 Python 环境。Python 3.7 是确保客户端运行的最小版本，Python 3.10 是推荐版本；
2. 部署 namesrv，broker 以及 [proxy](https://github.com/apache/rocketmq/tree/develop/proxy) 组件。

## 快速开始

我们使用 Poetry 作为依赖管理和发布的工具。你可以在 Poetry 的[官方网站]((https://python-poetry.org/))了解到关于它的更多信息。这里是一些在开发阶段你会使用到的 Poetry 命令：

```shell
# 创建并激活 python3 的虚拟环境
poetry env use python3
# 自动安装工程相关的依赖
poetry install
# 进入虚拟环境中的 shell
poetry shell
```

我们使用 pytest 来作为当前项目的测试框架，你可以通过直接执行 `pytest` 命令来运行所有的测试。

## 发布步骤

我们使用 PyPi 来帮助用户更好地在自己的工程中引入并使用客户端。为了将客户端发布到 PyPi，可以执行以下命令：

```shell
# 构建包
poetry build
# 将包发布到远程仓库
poetry publish -u username -p password
```

## 从 GitHub 安装

```shell
# 使用 Poetry 安装
poetry add git+https://github.com/apache/rocketmq-clients.git@master#subdirectory=python
# 或者 pip 安装
pip install -U "git+https://github.com/apache/rocketmq-clients/@master#egg=rocketmq&subdirectory=python"
```

## 目前进展

* 协议层代码生成完毕。
* `rpc_client.py` 完成部分。
