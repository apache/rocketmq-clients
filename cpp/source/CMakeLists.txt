add_subdirectory(admin)
add_subdirectory(base)
add_subdirectory(client)
add_subdirectory(concurrent)
add_subdirectory(log)
add_subdirectory(scheduler)

add_library(rocketmq
            $<TARGET_OBJECTS:admin>
            $<TARGET_OBJECTS:base>
            $<TARGET_OBJECTS:concurrent>
            $<TARGET_OBJECTS:log>
            $<TARGET_OBJECTS:scheduler>)