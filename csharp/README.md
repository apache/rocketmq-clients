# The C# Implementation of Apache RocketMQ Client

## Introduction

Here is the C# implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/).

## Architecture

Basically, this project would follow the same paradigm of [rocketmq-client-cpp v5.0.0](https://github.com/apache/rocketmq-client-cpp/tree/main). Namely, we would build the whole client following protocols described in [rocketmq-apis](https://github.com/apache/rocketmq-apis) on top of [gRPC-dotnet](https://github.com/grpc/grpc-dotnet), utilizing [Protocol buffers](https://developers.google.com/protocol-buffers) to serialize and deserialize data in transmission.

## How to build

Layout of this project roughly follows [this guide](https://docs.microsoft.com/en-us/dotnet/core/tutorials/library-with-visual-studio-code?pivots=dotnet-5-0). The solution contains a class library, a unit test module and an example console module.

1. Install dotnet tool chains following instructions [here](https://dotnet.microsoft.com/en-us/download);
2. Visual Studio Code with official C# plugin is used during development;

Assuming you are at the home of this repository,

### Build

```sh
dotnet build
```

### Run Unit Tests

```sh
dotnet test -l "console;verbosity=detailed"
```

### Run Examples

```sh
dotnet run -p examples
```
