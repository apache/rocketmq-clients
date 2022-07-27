set(PROTO_FILES
        apache/rocketmq/v2/admin.proto
        apache/rocketmq/v2/definition.proto
        apache/rocketmq/v2/service.proto)

add_library(proto ${PROTO_FILES})
target_link_libraries(proto
        PUBLIC
            protobuf::libprotobuf
            gRPC::grpc
            gRPC::grpc++)
target_include_directories(proto PUBLIC ${CMAKE_CURRENT_BINARY_DIR})

#
# Compile protobuf and grpc files in myproto target to cpp
#
get_target_property(grpc_cpp_plugin_location gRPC::grpc_cpp_plugin LOCATION)
protobuf_generate(TARGET proto LANGUAGE cpp)
protobuf_generate(TARGET proto LANGUAGE grpc GENERATE_EXTENSIONS .grpc.pb.h .grpc.pb.cc PLUGIN "protoc-gen-grpc=${grpc_cpp_plugin_location}")