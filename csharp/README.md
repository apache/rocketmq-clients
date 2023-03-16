# The .NET Implementation of Apache RocketMQ Client

English | [简体中文](https://github.com/apache/rocketmq-clients/blob/master/csharp/README-CN.md)
| [RocketMQ Website](https://rocketmq.apache.org/)

## Supported .NET Versions

.NET 5+ and .NET Core 3.1 is supported.

Due to the release of .NET 5 in 2020, which unified .NET Framework and .NET Core, and has gradually become the
mainstream platform for .NET development. We strongly recommend using .NET 5+ to access RocketMQ.

We also support access to RocketMQ using .NET Core 3.1. Note: If you want to use .NET Core 3.1 and want to disable
TLS/SSL by `Org.Apache.Rocketmq.ClientConfig.Builder.EnableSsl(false)`, add the following code before you run.

```csharp
// Only necessary if you want to disable TLS/SSL on .NET Core 3.1
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true)
```

See more details about .NET 5 from [Introducing .NET 5](https://devblogs.microsoft.com/dotnet/introducing-net-5/).

## Architecture

The client would be developed using the protocols outlined in [rocketmq-apis](https://github.com/apache/rocketmq-apis)
and built on [gRPC-dotnet](https://github.com/grpc/grpc-dotnet), leveraging Protocol Buffers for data serialization and
deserialization during transmission.

## Quickstart & Build

Use the command below to add client into your dependencies.

```sh
dotnet add package RocketMQ.Client
```

You can obtain the latest version of `RocketMQ.Client`
from [NuGet Gallery](https://www.nuget.org/packages/RocketMQ.Client). To assist with getting started quickly and working
with various message types and clients, we offer examples [here](./examples).

Layout of this project roughly
follows [this guide](https://docs.microsoft.com/en-us/dotnet/core/tutorials/library-with-visual-studio-code?pivots=dotnet-5-0)
. The solution contains a class library, a unit test module and an example console module. Assuming you are at the home
of this repository:

```sh
# build the project
dotnet build
# run unit tests
dotnet test -l "console;verbosity=detailed"
```

## Logging System

We use [NLog](https://nlog-project.org/) as our logging implementation. Similar to the Java binding, we allow the use of
environment variables to customize the related configuration:

* `rocketmq_log_level`: Log output level, default is INFO.
* `rocketmq_log_root`: The root directory of the log output. The default path is `$HOME/logs/rocketmq`, so the full path
  is `$HOME/logs/rocketmq/rocketmq-client.log`.
* `rocketmq_log_file_maxIndex`: The maximum number of log files to keep. The default is 10, and the size of a single log
  file is limited to 64 MB. Adjustment is not supported yet.

Specifically, by setting `mq_consoleAppender_enabled` to true, you can output client logs to the console simultaneously
if you need debugging.

## Publishing Steps

1. Open the command line, and change the directory to the project folder that you want to package.
2. Run the `dotnet pack --configuration Release` command. This will create a NuGet package in the `bin/Release` folder
   of the project.
3. To upload the package to NuGet, go to the NuGet website and sign in. Click on the "Upload" button and select the
   package file from the `bin/Release` folder.
4. Follow the instructions on the website to complete the upload process. Once the package is uploaded, it will be
   available for others to download and use.
