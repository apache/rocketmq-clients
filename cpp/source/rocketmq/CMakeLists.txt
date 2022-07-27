file(GLOB SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(impl OBJECT ${SRC_FILES})
target_include_directories(impl
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(impl
        PRIVATE
            api
            absl::strings
            asio
            base
            fmt
            proto
            client
            opencensus_api
            opencensus_proto
            rocketmq_stats
            rocketmq_trace
            scheduler
            spdlog
        )