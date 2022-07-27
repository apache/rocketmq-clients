add_library(opencensus_api INTERFACE)
target_include_directories(opencensus_api INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})

add_subdirectory(opencensus)