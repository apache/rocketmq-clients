# The .NET Implementation of Apache RocketMQ Client

Here is the .NET implementation of the client for [Apache RocketMQ](https://rocketmq.apache.org/).

## Supported .NET Versions

Due to the release of .NET 5 in 2020, which unified .NET Framework and .NET Core, and has gradually become the mainstream platform for .NET development, the RocketMQ client will support .NET 5 and later versions.

See more details about .NET 5 from [Introducing .NET 5](https://devblogs.microsoft.com/dotnet/introducing-net-5/).

## Architecture

The client would be developed using the protocols outlined in [rocketmq-apis](https://github.com/apache/rocketmq-apis) and built on [gRPC-dotnet](https://github.com/grpc/grpc-dotnet), leveraging Protocol Buffers for data serialization and deserialization during transmission.

## Quickstart

```sh
dotnet add package RocketMQ.Client
```

You can obtain the latest version of `RocketMQ.Client` from [NuGet Gallery](https://www.nuget.org/packages/RocketMQ.Client). To assist with getting started quickly and working with various message types and clients, we offer additional code [here](./examples) here.

## Build

Layout of this project roughly follows [this guide](https://docs.microsoft.com/en-us/dotnet/core/tutorials/library-with-visual-studio-code?pivots=dotnet-5-0). The solution contains a class library, a unit test module and an example console module.

Assuming you are at the home of this repository:

```sh
# build the project
dotnet build
# run unit tests
dotnet test -l "console;verbosity=detailed"
```
