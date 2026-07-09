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
add_rocketmq_test(lite_message_test LiteMessageTest.cpp)
add_rocketmq_test(lite_subscription_manager_test LiteSubscriptionManagerTest.cpp)
add_rocketmq_test(optional_test OptionalTest.cpp)
add_rocketmq_test(priority_message_test PriorityMessageTest.cpp)
add_rocketmq_test(send_context_test SendContextTest.cpp)
add_rocketmq_test(static_name_server_resolver_test StaticNameServerResolverTest.cpp)
add_rocketmq_test(time_test TimeTest.cpp)
