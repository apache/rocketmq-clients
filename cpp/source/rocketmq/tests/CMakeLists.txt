set(ROCKETMQ_TEST_INCLUDE_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}/../mocks/include
    ${CMAKE_CURRENT_SOURCE_DIR}/../../client/mocks/include
    ${ROCKETMQ_INTERNAL_INCLUDE_DIRS})

macro(add_rocketmq_test TEST_NAME TEST_SRC)
    add_executable(${TEST_NAME} ${TEST_SRC})
    target_include_directories(${TEST_NAME} PRIVATE ${ROCKETMQ_TEST_INCLUDE_DIRS})
    target_link_libraries(${TEST_NAME} PRIVATE ${ROCKETMQ_TEST_LINK_LIBS})
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endmacro()

add_rocketmq_test(client_impl_test ClientImplTest.cpp)
add_rocketmq_test(consume_message_service_test ConsumeMessageServiceTest.cpp)
add_rocketmq_test(optional_test OptionalTest.cpp)
add_rocketmq_test(priority_message_test PriorityMessageTest.cpp)
add_rocketmq_test(send_context_test SendContextTest.cpp)
add_rocketmq_test(static_name_server_resolver_test StaticNameServerResolverTest.cpp)
add_rocketmq_test(time_test TimeTest.cpp)
add_rocketmq_test(topic_publish_info_test TopicPublishInfoTest.cpp)
add_rocketmq_test(fifo_producer_partition_test FifoProducerPartitionTest.cpp)
add_rocketmq_test(producer_impl_test ProducerImplTest.cpp)
add_rocketmq_test(process_queue_impl_test ProcessQueueImplTest.cpp)
add_rocketmq_test(consume_task_test ConsumeTaskTest.cpp)
add_rocketmq_test(simple_consumer_impl_test SimpleConsumerImplTest.cpp)
add_rocketmq_test(push_consumer_impl_test PushConsumerImplTest.cpp)
