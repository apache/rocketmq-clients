add_library(fmt INTERFACE)
target_include_directories(fmt
        INTERFACE
            ${CMAKE_CURRENT_SOURCE_DIR}/include)