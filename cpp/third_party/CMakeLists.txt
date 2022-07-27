add_subdirectory(asio/1.18.2)
add_subdirectory(filesystem/1.5.12)
add_subdirectory(fmt/9.0.0)
add_subdirectory(spdlog/1.10.0)

include(OpenCensusHelpers)
add_subdirectory(opencensus/0.5.0-alpha)
add_subdirectory(opencensus/proto)