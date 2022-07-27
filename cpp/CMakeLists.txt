cmake_minimum_required(VERSION 3.13)
project(rocketmq)
set(CMAKE_CXX_STANDARD 11)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
find_package(gRPC REQUIRED)