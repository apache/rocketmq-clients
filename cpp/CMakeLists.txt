cmake_minimum_required(VERSION 3.16)
project(rocketmq VERSION 5.1.0 LANGUAGES CXX)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

include(GNUInstallDirs)
list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)
# Minimum versions aligned with the gRPC 1.46.x dependency chain (gRPC 1.46 bundles protobuf 3.19)
find_package(protobuf 3.19 CONFIG REQUIRED)
find_package(gRPC 1.46 CONFIG REQUIRED)
find_package(absl REQUIRED)
find_package(OpenSSL 1.1 REQUIRED)

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

option(BUILD_TESTS "Build unit tests or not" OFF)
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

# Install targets

include(CMakePackageConfigHelpers)

# Shared library: self-contained, exported for downstream find_package()
# Note: static library (rocketmq) is not installed — on Windows its .lib
# would collide with the shared library's import library (also .lib).
install(TARGETS rocketmq_shared
    EXPORT rocketmq-targets
    LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
    ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
    RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)

install(DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/include/rocketmq
    DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
    FILES_MATCHING PATTERN "*.h"
    PATTERN "Tracing.h" EXCLUDE
)

install(EXPORT rocketmq-targets
    FILE rocketmq-targets.cmake
    NAMESPACE rocketmq::
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/rocketmq
)

configure_package_config_file(
    ${CMAKE_CURRENT_SOURCE_DIR}/cmake/rocketmq-config.cmake.in
    ${CMAKE_CURRENT_BINARY_DIR}/rocketmq-config.cmake
    INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/rocketmq
)
write_basic_package_version_file(
    ${CMAKE_CURRENT_BINARY_DIR}/rocketmq-config-version.cmake
    VERSION ${PROJECT_VERSION}
    COMPATIBILITY SameMajorVersion
)
install(FILES
    ${CMAKE_CURRENT_BINARY_DIR}/rocketmq-config.cmake
    ${CMAKE_CURRENT_BINARY_DIR}/rocketmq-config-version.cmake
    DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/rocketmq
)

# Uninstall target
if(NOT TARGET uninstall)
    configure_file(
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/cmake_uninstall.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
        IMMEDIATE @ONLY
    )
    add_custom_target(uninstall
        COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake
    )
endif()
