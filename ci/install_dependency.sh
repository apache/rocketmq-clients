#!/bin/sh
error_exit() {
  echo "ERROR: $1 !"
  exit 1
}

copy_dependency_from_docker() {
  docker_id=$1
  docker_path=$2
  local_path=$3

  mkdir -p $local_path
  docker cp $docker_id:$docker_path $local_path
}

docker_id=$(docker create aaronai/rocketmq_java:v20210402_175624)
docker_maven_repository_path="/home/rocketmq/.m2/repository/"

grpc_sub_path="io/grpc"
copy_dependency_from_docker $docker_id $docker_maven_repository_path$grpc_sub_path $HOME/.m2/repository/io/
if [ $? -eq 0 ]; then
  echo "Copy dependency of grpc from docker successfully"
else
  error_exit "Failed to copy dependency of grpc from docker"
fi

protobuf_sub_path="com/google/protobuf"
copy_dependency_from_docker $docker_id $docker_maven_repository_path$protobuf_sub_path $HOME/.m2/repository/com/google/
if [ $? -eq 0 ]; then
  echo "Copy dependency of protobuffer from docker successfully"
else
  error_exit "Failed to copy dependency of protobuffer from docker"
fi
