add_executable(scheduler_test SchedulerTest.cpp)
target_include_directories(scheduler_test PRIVATE ${ROCKETMQ_INTERNAL_INCLUDE_DIRS})
target_link_libraries(scheduler_test PRIVATE ${ROCKETMQ_TEST_LINK_LIBS})
add_test(NAME scheduler_test COMMAND scheduler_test)
