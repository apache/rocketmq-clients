#!/bin/bash

set -e

IMAGE_TAG="v$(date +%Y%m%d_%H%M%S)"
REMOTE_REGISTRY="aaronai/rocketmq_java"

docker build -t rocketmq-java:latest -f Dockerfile .
docker tag rocketmq-java:latest "$REMOTE_REGISTRY":"$IMAGE_TAG"
docker tag rocketmq-java:latest "$REMOTE_REGISTRY":latest
#docker push "$REMOTE_REGISTRY":"$IMAGE_TAG"
#docker push "$REMOTE_REGISTRY":latest
