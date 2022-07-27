file(GLOB SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(client OBJECT ${SRC_FILES})
target_include_directories(client
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(client
        PRIVATE
            api
            base
            proto
            fmt
            gRPC::grpc
            OpenSSL::SSL
            spdlog)