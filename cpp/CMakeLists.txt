cmake_minimum_required(VERSION 3.16)
project(rocketmq)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
find_package(protobuf CONFIG REQUIRED)
find_package(gRPC CONFIG REQUIRED)
find_package(absl REQUIRED)
find_package(OpenSSL REQUIRED)

# Thread safety annotation macros
# Map legacy names to ABSL_-prefixed names (newer abseil >= 20230125)
# so that existing code using GUARDED_BY, LOCKS_EXCLUDED, etc. continues to work.
# Function-style macros cannot be passed via -D on the command line, so we
# force-include a compatibility header that defines them instead.
add_compile_options(
    "-include"
    "${CMAKE_CURRENT_SOURCE_DIR}/source/base/include/thread_annotations_compat.h"
)

if(NOT EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/proto/apache/rocketmq/v2/definition.proto")
    message(FATAL_ERROR "Proto files not found. Run: git submodule update --init --recursive")
endif()
add_subdirectory(proto)

add_library(api INTERFACE)
target_include_directories(api INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)

add_subdirectory(third_party)

add_subdirectory(source)

option(BUILD_EXAMPLES "Build example programs or not" ON)
if (BUILD_EXAMPLES)
    find_package(gflags REQUIRED)
    find_package(ZLIB REQUIRED)
    add_subdirectory(examples)
endif ()

option(BUILD_TESTS "Build unit tests or not" ON)
if (BUILD_TESTS)
    enable_testing()
    include(FetchContent)
    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG release-1.11.0
    )
    set(BUILD_GMOCK ON CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)
    FetchContent_MakeAvailable(googletest)

    set(ROCKETMQ_INTERNAL_INCLUDE_DIRS
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_SOURCE_DIR}/source/admin/include
        ${PROJECT_SOURCE_DIR}/source/base/include
        ${PROJECT_SOURCE_DIR}/source/client/include
        ${PROJECT_SOURCE_DIR}/source/concurrent/include
        ${PROJECT_SOURCE_DIR}/source/log/include
        ${PROJECT_SOURCE_DIR}/source/rocketmq/include
        ${PROJECT_SOURCE_DIR}/source/scheduler/include
        ${PROJECT_SOURCE_DIR}/source/stats/include
        ${PROJECT_SOURCE_DIR}/source/trace/include
        ${PROJECT_SOURCE_DIR}/third_party/asio/1.18.2/include
        ${PROJECT_SOURCE_DIR}/third_party/fmt/9.0.0/include
        ${PROJECT_SOURCE_DIR}/third_party/spdlog/1.10.0/include
        ${PROJECT_SOURCE_DIR}/third_party/filesystem/1.5.12/include
        ${PROJECT_BINARY_DIR}/proto
    )
    set(ROCKETMQ_TEST_LINK_LIBS rocketmq proto gtest_main gmock)

    add_subdirectory(source/admin/tests)
    add_subdirectory(source/base/tests)
    add_subdirectory(source/client/tests)
    add_subdirectory(source/concurrent/tests)
    add_subdirectory(source/rocketmq/tests)
    add_subdirectory(source/scheduler/tests)
    add_subdirectory(source/stats/tests)
endif ()
