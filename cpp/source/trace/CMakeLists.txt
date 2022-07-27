add_library(rocketmq_trace OBJECT TracingUtility.cpp)
target_include_directories(rocketmq_trace
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(rocketmq_trace
        PRIVATE
            api
            base
            client
            fmt
            opencensus::stats
            proto
            spdlog)