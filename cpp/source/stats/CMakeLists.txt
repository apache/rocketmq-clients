file(GLOB SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(rocketmq_stats OBJECT ${SRC_FILES})
target_include_directories(rocketmq_stats
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(rocketmq_stats
        PRIVATE
            api
            base
            client
            fmt
            opencensus::stats
            opencensus_proto
            proto
            spdlog
            scheduler)