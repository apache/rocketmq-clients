file(GLOB SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)

add_library(impl OBJECT ${SRC_FILES})
target_include_directories(impl
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(impl
        PRIVATE
            api
            absl::strings
            base
            fmt
            proto
            client
            opencensus_api
            scheduler
            spdlog
        )