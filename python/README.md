# The Python Implementation of Apache RocketMQ Client

English | [简体中文](README-CN.md) | [RocketMQ Website](https://rocketmq.apache.org/)

## Overview

Here is the Python implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/). Different from the [remoting-based client](https://github.com/apache/rocketmq/tree/develop/client), the current implementation is based on separating architecture for computing and storage, which is the more recommended way to access the RocketMQ service.

Here are some preparations you may need to know (or refer to [here](https://rocketmq.apache.org/docs/quickStart/02quickstart/)).

1. Python 3.7 is the minimum version required, Python 3.10 is the recommended version.
2. Setup namesrv, broker, and [proxy](https://github.com/apache/rocketmq/tree/develop/proxy).

## Getting Started

We are using Poetry as the dependency management & publishing tool. You can find out more details about Poetry from its [website](https://python-poetry.org/). Here is the related command of Poetry you may use for development.

```shell
# Create a virtual environment and activate it.
poetry env use python3
# Installs the project dependencies.
poetry install
# Spawns a shell within the virtual environment.
poetry shell
```

We use pytest as the testing framework for the current project, and you can execute `pytest` directly to run all tests.

## Publishing Steps

We utilize PyPI to help users easily introduce and use the Python client in their projects. To publish a package to PyPI, please register an account in advance, then execute the following command.

```shell
# Builds a package, as a tarball and a wheel by default.
poetry build
# Publishes a package to a remote repository.
poetry publish -u username -p password
```

## Current Progress

* Protocol layer code generation is completed.
* Partial completion of `rpc_client.py`.
