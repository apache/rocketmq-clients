# .NET 客户端

[![NuGet Gallery][nuget-image]][nuget-url]

[English](README.md) | 简体中文 | [RocketMQ 官网](https://rocketmq.apache.org/)

## 最小版本支持

支持 .NET 5+ 和 .NET Core 3.1。

由于 .NET 5 在 2020 年的发布，统一了.NET Framework 和 .NET Core ，并逐渐成为 .NET 开发的主流平台。我们强烈推荐使用 .NET 5+
访问 RocketMQ。

与此同时我们也支持 .NET Core 3.1。注意：如果您想使用 .NET Core 3.1 接入 RocketMQ
且想通过 `Org.Apache.Rocketmq.ClientConfig.Builder.EnableSsl(false)` 关闭 TLS/SSL，请在运行之前添加以下代码。

```csharp
// .NET Core 3.1 环境下想关闭 TLS/SSL 时需要添加此代码
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true)
```

关于 .NET 5 的更多介绍，可以参照 [Introducing .NET 5](https://devblogs.microsoft.com/dotnet/introducing-net-5/)。

## 概述

当前客户端基于 [rocketmq-apis](https://github.com/apache/rocketmq-apis)
中的协议约束和 [gRPC-dotnet](https://github.com/grpc/grpc-dotnet) 进行构建，使用 Protocol Buffers 作为序列化协议。

## 快速开始

使用下面的命令来将客户端添加到你项目的依赖中：

```sh
dotnet add package RocketMQ.Client
```

你可以从 [Nuget Gallery](https://www.nuget.org/packages/RocketMQ.Client) 从获取最新的 `RocketMQ.Client`
版本，我们提供了[代码示例](./examples)来帮助你快速开始。

## 构建

本项目的布局大致遵循[此处的指南](https://docs.microsoft.com/en-us/dotnet/core/tutorials/library-with-visual-studio-code?pivots=dotnet-5-0)
，解决方案内包含客户端类库，单元测试模块和示例代码模块。假设你处于当前项目的路径下：

```sh
# 构建项目
dotnet build
# 运行单元测试
dotnet test -l "console;verbosity=detailed"
```

## 日志系统

我们使用 [Microsoft.Extensions.Logging](https://learn.microsoft.com/en-us/dotnet/core/extensions/logging?tabs=command-line#non-host-console-app) 作为日志实现。

默认的 `LoggerFactory` 是 [NLog](https://nlog-project.org/)。与 Java 客户端类似，我们允许使用环境变量来自定义日志相关的配置：

* `rocketmq_log_level`：日志输出级别，默认为 INFO。
* `rocketmq_log_root`
  ：日志输出的根目录。默认路径为 `$HOME/logs/rocketmq`，因此完整路径为 `$HOME/logs/rocketmq/rocketmq-client.log`。
* `rocketmq_log_file_maxIndex`：要保留的日志文件的最大数量。默认值为 10，单个日志文件的大小限制为 64 MB。暂不支持调整。

如果你想使用自定义的 `LoggerFactory`，可以使用 `MqLogManager.UseLoggerFactory` 方法来配置。

```csharp
var loggerFactory = LoggerFactory.Create(builder =>
{
    builder
        .AddFilter("Org.Apache.Rocketmq", LogLevel.Warning)
        .AddConsole();
});
MqLogManager.UseLoggerFactory(loggerFactory);
```

## NuGet 包发布步骤

1. 打开命令行，进入 csharp 文件夹。
2. 执行 `dotnet pack --configuration Release` 命令. 这会创建对应的 NuGet 包到 `bin/Release` 文件夹；
3. 登录 NuGet Gallery 并登录，点击 `Upload` 按钮并将 nupkg 文件拖入提示框；
4. 按照 NuGet Gallery 的提示，完成后续步骤。

[nuget-image]: https://img.shields.io/nuget/v/RocketMQ.Client.svg?label=nuget
[nuget-url]: https://www.nuget.org/packages/RocketMQ.Client/
