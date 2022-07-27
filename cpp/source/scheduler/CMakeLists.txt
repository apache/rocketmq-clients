add_library(scheduler OBJECT SchedulerImpl.cpp)
target_include_directories(scheduler
        PRIVATE
            ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(scheduler
        PRIVATE
            absl::base
            api
            asio
            spdlog)
