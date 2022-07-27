add_library(asio INTERFACE)
target_include_directories(asio
        INTERFACE
            ${CMAKE_CURRENT_SOURCE_DIR}/include)