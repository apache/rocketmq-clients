# The Python Implementation of Apache RocketMQ Client

English | [简体中文](README-CN.md) | [RocketMQ Website](https://rocketmq.apache.org/)

## Overview

Here is the python implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/). Different from the [remoting-based client](https://github.com/apache/rocketmq/tree/develop/client), the current implementation is based on separating architecture for computing and storage, which is the more recommended way to access the RocketMQ service.

Here are some preparations you may need to know (or refer to [here](https://rocketmq.apache.org/docs/quickStart/02quickstart/https://rocketmq.apache.org/docs/quickStart/02quickstart/)).

1. Python 3.7 is the minimum version required, Python 3.10 is the recommended version.
2. Setup namesrv, broker, and [proxy](https://github.com/apache/rocketmq/tree/develop/proxy).

## Getting Started

To install the relevant dependency libraries, use the command "pip install -r requirements.txt" in the directory "rocketmq-clients/python".

## Current Progress

* Protocol layer code generation is completed.

* Partial completion of rpcClient.
