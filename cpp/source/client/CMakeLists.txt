file(GLOB SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(client OBJECT ${SRC_FILES})
target_include_directories(client
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(client
        PRIVATE
            api
            asio
            base
            proto
            fmt
            gRPC::grpc
            OpenSSL::SSL
            opencensus::stats
            opencensus::trace
            scheduler
            spdlog)