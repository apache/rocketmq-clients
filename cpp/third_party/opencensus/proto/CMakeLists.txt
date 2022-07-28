set(PROTO_FILES
        opencensus/proto/stats/v1/stats.proto
        opencensus/proto/metrics/v1/metrics.proto
        opencensus/proto/agent/metrics/v1/metrics_service.proto
        opencensus/proto/agent/common/v1/common.proto
        opencensus/proto/resource/v1/resource.proto)

add_library(opencensus_proto ${PROTO_FILES})
target_link_libraries(opencensus_proto
        PUBLIC
        protobuf::libprotobuf
        gRPC::grpc
        gRPC::grpc++)
target_include_directories(opencensus_proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

#
# Compile protobuf and grpc files in myproto target to cpp
#
get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
protobuf_generate(TARGET opencensus_proto LANGUAGE cpp)
protobuf_generate(TARGET opencensus_proto LANGUAGE grpc GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}")