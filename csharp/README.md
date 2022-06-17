[![CI](https://github.com/lizhanhui/rocketmq-client-csharp/actions/workflows/main.yml/badge.svg)](https://github.com/lizhanhui/rocketmq-client-csharp/actions/workflows/main.yml)

Introduction
--------------
Project rocketmq-client-csharp is targeted to implement C# binding in native C# code. At the current moment, it is still a work-in-progress project. Do not use it in production till it grows mature enough.

Architecture
--------------
Basically, this project would follow the same paradigm of [rocketmq-client-cpp v5.0.0](https://github.com/apache/rocketmq-client-cpp/tree/main). Namely, we would build the whole client following protocols described in [rocketmq-apis](https://github.com/apache/rocketmq-apis) on top of [gRPC-dotnet](https://github.com/grpc/grpc-dotnet), utilizing [Protocol buffers](https://developers.google.com/protocol-buffers) to serialize and deserialize data in transmission.


How to build
-----------------
Layout of this project roughly follows [this guide](https://docs.microsoft.com/en-us/dotnet/core/tutorials/library-with-visual-studio-code?pivots=dotnet-5-0). The solution contains a class library, a unit test module and an example console module.

1. Install dotnet tool chains following instructions [here](https://dotnet.microsoft.com/en-us/download);
2. Visual Studio Code with official C# plugin is used during development;

Assuming you are at the home of this repository,
#### Build

```sh
dotnet build
```

#### Run Unit Tests
```sh
dotnet test -l "console;verbosity=detailed"
```

#### Run Examples
```sh
dotnet run -p examples
```

License
------------------
This project follows [Apache License Version 2.0](./LICENSE). 

How to contribute
------------------
Similar to other Apache RocketMQ projects, we welcome contributions in various ways, from filing a bug report, correcting type error, document writing to complete feature implementation. Any attempt to make this project better is welcome.

If this project catches your attention, do not hesitate to make a pull request.