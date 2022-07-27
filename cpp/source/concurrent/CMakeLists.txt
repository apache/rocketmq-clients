add_library(concurrent OBJECT CountdownLatch.cpp)
target_include_directories(concurrent
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(concurrent
        PRIVATE
            absl::base
            api
            spdlog)