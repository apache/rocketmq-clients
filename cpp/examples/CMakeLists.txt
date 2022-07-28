add_executable(example_producer ExampleProducer.cpp)
target_link_libraries(example_producer
        PRIVATE
            api
            gflags
            rocketmq)