file(GLOB SRC_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cpp)
add_library(base OBJECT ${SRC_FILES})
target_include_directories(base
        PUBLIC
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(base
        PRIVATE
            absl::flat_hash_map
            api
            asio
            filesystem
            fmt
            OpenSSL::SSL
            proto
            spdlog)