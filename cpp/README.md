# The C++ Implementation of Apache RocketMQ Client

[![Codecov-cpp][codecov-cpp-image]][codecov-url]

## Introduction

This is the C++ client for [Apache RocketMQ](https://rocketmq.apache.org/) 5.x, built on top of [gRPC](https://grpc.io/) and [Protocol Buffers](https://developers.google.com/protocol-buffers). It follows the [rocketmq-apis](https://github.com/apache/rocketmq-apis) specification and provides Producer, FifoProducer, PushConsumer and SimpleConsumer APIs.

The proto definitions are shared via the `protos/` git submodule at the repository root. Make sure to clone with `--recursive` or run `git submodule update --init` before building.

## Prerequisites

- C++ compiler supporting C++11
- CMake 3.13+ or Bazel 5.2.0
- gRPC — RPC communication framework, also brings in protobuf (serialization), abseil (base library), and re2 (regex)
- OpenSSL development headers — TLS encrypted communication
- zlib development headers — message body compression

## How To Build

### Build with CMake (Recommended)

1. Install gRPC (v1.46.3) and its dependencies:

   ```shell
   # Install system dependencies (pick your distro)
   sudo apt install -y libssl-dev zlib1g-dev       # Debian/Ubuntu
   sudo yum install -y openssl-devel zlib-devel    # CentOS/RHEL/Alibaba Cloud Linux

   # Clone gRPC source with submodules
   git clone --recurse-submodules -b v1.46.3 --depth 1 \
     https://github.com/grpc/grpc.git /tmp/grpc

   # Build and install to /usr/local (system default)
   cd /tmp/grpc && mkdir build && cd build
   cmake -DCMAKE_BUILD_TYPE=Release \
         -DgRPC_INSTALL=ON \
         -DgRPC_BUILD_TESTS=OFF \
         -DgRPC_SSL_PROVIDER=package \
         -DgRPC_ZLIB_PROVIDER=package \
         ..
   make -j $(nproc)
   sudo make install
   ```

   Or install to a custom location (no sudo required):

   ```shell
   cmake -DCMAKE_INSTALL_PREFIX=$HOME/grpc ...
   make -j $(nproc) && make install
   ```

2. Install gflags (required for example programs):

   ```shell
   git clone -b v2.2.2 --depth 1 https://github.com/gflags/gflags.git /tmp/gflags
   cd /tmp/gflags && mkdir build && cd build
   cmake -DCMAKE_BUILD_TYPE=Release ..
   make -j $(nproc)
   sudo make install
   ```

   If you don't need examples, skip this step and pass `-DBUILD_EXAMPLES=OFF` when building.

3. Build the project:

   ```shell
   cd cpp && mkdir build && cd build
   cmake ..
   make -j $(nproc)
   ```

   If gRPC is installed to a non-default location, pass the path explicitly:

   ```shell
   cmake -DCMAKE_PREFIX_PATH=/path/to/grpc ..
   ```

4. Run unit tests (from the build directory):

   ```shell
   cd build
   ctest --output-on-failure -j$(nproc)
   ```

5. CMake options:

   | Option           | Default | Description                                          |
   | ---------------- | ------- | ---------------------------------------------------- |
   | `BUILD_TESTS`    | ON      | Build unit tests (requires googletest, auto-fetched) |
   | `BUILD_EXAMPLES` | ON      | Build example programs (requires gflags)             |

   To skip tests and examples:

   ```shell
   cmake -DBUILD_TESTS=OFF -DBUILD_EXAMPLES=OFF ..
   ```

### Build with Bazel

```shell
cd cpp
bazel build //...
bazel test //...
```

## Testing

### Run a single test case

1. List all test cases:

   ```shell
   ./build/your_test --gtest_list_tests
   ```

2. Run a specific test case:

   ```shell
   ./build/your_test --gtest_filter=TestSuite.TestName
   ```

### Run tests multiple times

```shell
bazel test --runs_per_test=10 //...
```

### Test Coverage

Generate coverage data and HTML report:

```shell
bazel coverage -s \
  --instrument_test_targets \
  --experimental_cc_coverage \
  --combined_report=lcov \
  --coverage_report_generator=@bazel_tools//tools/test/CoverageOutputGenerator/java/com/google/devtools/coverageoutputgenerator:Main \
  //source/...

genhtml bazel-out/_coverage/_coverage_report.dat \
  --output-directory coverage_html
```

## Run Examples

All commands should run from the `cpp/` directory.

### Publish messages

```shell
# Standard messages (sync)
bazel run //examples:example_producer -- \
  --topic=YOUR_TOPIC --access_point=SERVICE_ACCESS_POINT --total=16

# Standard messages (async)
bazel run //examples:example_producer_with_async -- \
  --topic=YOUR_TOPIC --access_point=SERVICE_ACCESS_POINT --total=16

# FIFO messages
bazel run //examples:example_producer_with_fifo_message -- \
  --topic=YOUR_TOPIC --access_point=SERVICE_ACCESS_POINT --total=16

# Transactional messages
bazel run //examples:example_producer_with_transactional_message -- \
  --topic=YOUR_TOPIC --access_point=SERVICE_ACCESS_POINT --total=16
```

### Consume messages

```shell
# Push consumer (message listener)
bazel run //examples:example_push_consumer -- \
  --topic=YOUR_TOPIC --access_point=SERVICE_ACCESS_POINT --group=YOUR_GROUP_ID

# Simple consumer (pull-based)
bazel run //examples:example_simple_consumer -- \
  --topic=YOUR_TOPIC --access_point=SERVICE_ACCESS_POINT --group=YOUR_GROUP_ID
```

## Code Style

Based on [Google C++ Code Style](https://google.github.io/styleguide/cppguide.html), enforced by `.clang-format` and `.clang-tidy`.

- C++11 standard, compatible with [gRPC's compiler matrix](https://github.com/grpc/grpc/blob/master/BUILDING.md)
- Exceptions only in public API wrapper classes; internal code uses `std::error_code`
- Smart pointers preferred; raw pointers only when necessary

Format code:

```shell
./tools/format.sh
```

## Dependency Management

This SDK is built on top of gRPC. Before introducing a new third-party dependency, check [gRPC deps](https://github.com/grpc/grpc/blob/master/bazel/grpc_deps.bzl) and [gRPC extra deps](https://github.com/grpc/grpc/blob/master/bazel/grpc_extra_deps.bzl) first.

All Bazel dependencies should use `maybe()` to ensure back-off compatibility.

## IDE Setup

### VSCode + Clangd

Generate `compile_commands.json` for clangd:

```shell
./tools/gen_compile_commands.sh
```

### CLion + Bazel Plugin

CLion is supported via the [Bazel plugin](https://plugins.jetbrains.com/plugin/8609-bazel).

[codecov-cpp-image]: https://img.shields.io/codecov/c/gh/apache/rocketmq-clients/master?flag=cpp&label=CPP%20Coverage&logo=codecov
[codecov-url]: https://app.codecov.io/gh/apache/rocketmq-clients
