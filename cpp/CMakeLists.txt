cmake_minimum_required(VERSION 3.19)
project(rocketmq)
set(CMAKE_CXX_STANDARD 11)

set(gRPC_DEBUG ON)

# Assume gRPC is installed $HOME/grpc
list(APPEND CMAKE_PREFIX_PATH $ENV{HOME}/grpc)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(absl REQUIRED)
find_package(OpenSSL REQUIRED)

add_subdirectory(proto)

add_library(api INTERFACE)
target_include_directories(api INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_subdirectory(third_party)

add_subdirectory(source)