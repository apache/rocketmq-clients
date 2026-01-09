
# golang pb generate:
protoc --go-grpc_out=. apache/rocketmq/v2/*.proto
protoc --go_out=. apache/rocketmq/v2/*.proto