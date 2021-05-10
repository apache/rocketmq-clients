#!/bin/bash

set -e

docker rm -f dummy >/dev/null 2>&1
docker create --name dummy aaronai/rocketmq_java:latest >/dev/null 2>&1

# install grpc to local repo.
GRPC_LOCAL_PATH="$HOME"/.m2/repository/io/grpc
mkdir -p "$HOME"/.m2/repository/io
rm -rf "$GRPC_LOCAL_PATH" >/dev/null 2>&1
docker cp dummy:/home/rocketmq/.m2/repository/io/grpc "$GRPC_LOCAL_PATH"

# install protobuf to local repo.
PROTOBUF_LOCAL_PATH="$HOME"/.m2/repository/com/google/protobuf/
mkdir -p "$HOME"/.m2/repository/com/google
rm -rf "$PROTOBUF_LOCAL_PATH" >/dev/null 2>&1
docker cp dummy:/home/rocketmq/.m2/repository/com/google/protobuf "$PROTOBUF_LOCAL_PATH"

docker rm -f dummy >/dev/null 2>&1
