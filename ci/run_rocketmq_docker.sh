#!/bin/bash

set -e

IMAGE_NAME="aaronai/rocketmq_java"
IMAGE_TAG="latest"
export ROCKETMQ_BUILD_IMAGE="${IMAGE_NAME}:${IMAGE_TAG}"

SOURCE_DIR="${PWD}"
DOCKER_HOME="/home/rocketmq"
SOURCE_MOUNT_DEST="${DOCKER_HOME}/rocketmq-java"

START_COMMAND=("/bin/bash" "-c" "bash -c 'cd /home/rocketmq/rocketmq-java && $*'")
docker run --rm \
  -v "${SOURCE_DIR}":"${SOURCE_MOUNT_DEST}" \
  "${ROCKETMQ_BUILD_IMAGE}" \
  "${START_COMMAND[@]}"
