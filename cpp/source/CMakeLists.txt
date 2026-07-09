add_subdirectory(admin)
add_subdirectory(base)
add_subdirectory(client)
add_subdirectory(concurrent)
add_subdirectory(log)
add_subdirectory(scheduler)
add_subdirectory(stats)
add_subdirectory(trace)
add_subdirectory(rocketmq)

add_library(rocketmq STATIC
            $<TARGET_OBJECTS:admin>
            $<TARGET_OBJECTS:base>
            $<TARGET_OBJECTS:client>
            $<TARGET_OBJECTS:concurrent>
            $<TARGET_OBJECTS:log>
            $<TARGET_OBJECTS:rocketmq_stats>
            $<TARGET_OBJECTS:rocketmq_trace>
            $<TARGET_OBJECTS:impl>
            $<TARGET_OBJECTS:scheduler>)

target_link_libraries(rocketmq
        PUBLIC
            absl::base
            gRPC::grpc++
            fmt
            proto
            opencensus::trace
            opencensus::stats
            opencensus_proto
            spdlog)
set_target_properties(rocketmq PROPERTIES ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})


add_library(rocketmq_shared SHARED
        $<TARGET_OBJECTS:admin>
        $<TARGET_OBJECTS:base>
        $<TARGET_OBJECTS:client>
        $<TARGET_OBJECTS:concurrent>
        $<TARGET_OBJECTS:log>
        $<TARGET_OBJECTS:rocketmq_stats>
        $<TARGET_OBJECTS:rocketmq_trace>
        $<TARGET_OBJECTS:impl>
        $<TARGET_OBJECTS:scheduler>)

target_link_libraries(rocketmq_shared
        PUBLIC
            absl::base
            gRPC::grpc++
            $<BUILD_INTERFACE:opencensus::trace>
        PRIVATE
            fmt
            proto
            opencensus::stats
            opencensus_proto
            spdlog)
target_include_directories(rocketmq_shared
        PUBLIC
            $<BUILD_INTERFACE:${PROJECT_SOURCE_DIR}/include>
            $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)
if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(VERSION_SCRIPT ${CMAKE_CURRENT_SOURCE_DIR}/exports.map)
    set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -Wl,--version-script=${VERSION_SCRIPT}")
    set_target_properties(rocketmq_shared
            PROPERTIES
                LINK_DEPENDS ${VERSION_SCRIPT})
endif()
set_target_properties(rocketmq_shared
        PROPERTIES
            VERSION ${PROJECT_VERSION}
            SOVERSION ${PROJECT_VERSION_MAJOR}
            LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}
            LIBRARY_OUTPUT_NAME rocketmq)
