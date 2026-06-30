set(CLIENT_MOCK_DIRS
    ${CMAKE_CURRENT_SOURCE_DIR}/../mocks/include
    ${ROCKETMQ_INTERNAL_INCLUDE_DIRS})

macro(add_client_test TEST_NAME TEST_SRC)
    add_executable(${TEST_NAME} ${TEST_SRC})
    target_include_directories(${TEST_NAME} PRIVATE ${CLIENT_MOCK_DIRS})
    target_link_libraries(${TEST_NAME} PRIVATE ${ROCKETMQ_TEST_LINK_LIBS} OpenSSL::SSL)
    add_test(NAME ${TEST_NAME} COMMAND ${TEST_NAME})
endmacro()

add_client_test(tls_helper_test TlsHelperTest.cpp)
# RpcClientTest requires google/rpc/code.pb.h (googleapis, only available via Bazel)
# add_client_test(rpc_client_test RpcClientTest.cpp)
add_client_test(client_test ClientTest.cpp)
add_client_test(client_manager_test ClientManagerTest.cpp)
add_client_test(topic_assignment_info_test TopicAssignmentInfoTest.cpp)
# TracingUtilityTest requires OpenTelemetry headers (only available via Bazel)
# add_client_test(tracing_utility_test TracingUtilityTest.cpp)
