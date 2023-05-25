# The Python Implementation of Apache RocketMQ Client

English | [简体中文](README-CN.md) | [RocketMQ Website](https://rocketmq.apache.org/)

## Overview

Here is the Python implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/). Different from the [remoting-based client](https://github.com/apache/rocketmq/tree/develop/client), the current implementation is based on separating architecture for computing and storage, which is the more recommended way to access the RocketMQ service.

Here are some preparations you may need to know (or refer to [here](https://rocketmq.apache.org/docs/quickStart/02quickstart/https://rocketmq.apache.org/docs/quickStart/02quickstart/)).

1. Python 3.7 is the minimum version required, Python 3.10 is the recommended version.
2. Setup namesrv, broker, and [proxy](https://github.com/apache/rocketmq/tree/develop/proxy).

## Getting Started

Clone the current repository to your local machine and set up a virtual environment for development, which will help you manage dependencies more efficiently. Follow the steps below:

Navigate to the `python` subdirectory and execute the command below to create a new virtual environment:

```sh
python3 -m venv myvenv
```

Activate the virtual environment. The activation method depends on your operating system:

* For Windows, execute: `myvenv\Scripts\activate.bat`
* For macOS/Linux: execute: `source myvenv/bin/activate`

Install the required dependency libraries by executing the following command:

```sh
pip install -r requirements.txt
```

## Current Progress

* Protocol layer code generation is completed.

* Partial completion of rpcClient.
