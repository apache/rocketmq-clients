/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include <apache/rocketmq/v2/definition.pb.h>

#include <chrono>
#include <memory>
#include <system_error>

#include "ClientManagerImpl.h"
#include "RpcClientMock.h"
#include "SendResult.h"
#include "gtest/gtest.h"
#include "rocketmq/ErrorCode.h"

ROCKETMQ_NAMESPACE_BEGIN

class ClientManagerTest : public testing::Test {
public:
  void SetUp() override {
    client_manager_ = std::make_shared<ClientManagerImpl>(resource_namespace_);
    client_manager_->start();
    rpc_client_ = std::make_shared<testing::NiceMock<RpcClientMock>>();
    ON_CALL(*rpc_client_, ok).WillByDefault(testing::Return(true));
    client_manager_->addRpcClient(target_host_, rpc_client_);
    metadata_.insert({"foo", "bar"});
    metadata_.insert({"name", "Donald.J.Trump"});
  }

  void TearDown() override {
    client_manager_->shutdown();
  }

protected:
  std::string resource_namespace_{"mq://test"};
  std::string topic_{"TestTopic"};
  std::string target_host_{"ipv4:10.0.0.0:10911"};
  std::shared_ptr<ClientManagerImpl> client_manager_;
  std::shared_ptr<testing::NiceMock<RpcClientMock>> rpc_client_;
  absl::Duration io_timeout_{absl::Seconds(3)};
  Metadata metadata_;
  std::string message_body_{"Message body"};
  std::string tag_{"TagA"};
  std::string key_{"key-0"};
};

TEST_F(ClientManagerTest, testBasic) {
  // Ensure that start/shutdown works well.
}

TEST_F(ClientManagerTest, testResolveRoute) {
  auto rpc_cb = [](const QueryRouteRequest& request, InvocationContext<QueryRouteResponse>* invocation_context) {
    auto partition = new rmq::MessageQueue();
    partition->mutable_topic()->set_resource_namespace(request.topic().resource_namespace());
    partition->mutable_topic()->set_name(request.topic().name());
    partition->mutable_broker()->set_name("broker-0");
    partition->mutable_broker()->set_id(0);
    auto address = new rmq::Address();
    address->set_host("10.0.0.1");
    address->set_port(10911);
    partition->mutable_broker()->mutable_endpoints()->set_scheme(rmq::AddressScheme::IPv4);
    partition->mutable_broker()->mutable_endpoints()->mutable_addresses()->AddAllocated(address);
    invocation_context->response.mutable_message_queues()->AddAllocated(partition);

    invocation_context->onCompletion(true);
  };
  EXPECT_CALL(*rpc_client_, asyncQueryRoute).Times(testing::AtLeast(1)).WillRepeatedly(testing::Invoke(rpc_cb));

  bool completed = false;
  absl::Mutex mtx;
  absl::CondVar cv;

  QueryRouteRequest request;
  request.mutable_topic()->set_resource_namespace(resource_namespace_);
  request.mutable_topic()->set_name(topic_);
  auto callback = [&](const std::error_code& ec, const TopicRouteDataPtr&) {
    absl::MutexLock lk(&mtx);
    completed = true;
    cv.SignalAll();
  };
  client_manager_->resolveRoute(target_host_, metadata_, request, absl::ToChronoMilliseconds(io_timeout_), callback);
  {
    absl::MutexLock lk(&mtx);
    cv.WaitWithDeadline(&mtx, absl::Now() + absl::Seconds(3));
  }
  EXPECT_TRUE(completed);
}

TEST_F(ClientManagerTest, testQueryAssignment) {
  bool completed = false;
  absl::Mutex mtx;
  absl::CondVar cv;

  auto mock_query_assignment = [&](const QueryAssignmentRequest& request,
                                   InvocationContext<QueryAssignmentResponse>* invocation_context) {
    absl::MutexLock lk(&mtx);
    completed = true;
    cv.SignalAll();
    invocation_context->onCompletion(true);
  };

  EXPECT_CALL(*rpc_client_, asyncQueryAssignment)
      .Times(testing::AtLeast(1))
      .WillRepeatedly(testing::Invoke(mock_query_assignment));
  QueryAssignmentRequest request;
  bool callback_invoked = false;
  auto callback = [&](const std::error_code& ec, const QueryAssignmentResponse& response) { callback_invoked = true; };

  client_manager_->queryAssignment(target_host_, metadata_, request, absl::ToChronoMilliseconds(io_timeout_), callback);

  {
    absl::MutexLock lk(&mtx);
    if (!completed) {
      cv.WaitWithDeadline(&mtx, absl::Now() + absl::Seconds(3));
    }
  }
  EXPECT_TRUE(completed);
  EXPECT_TRUE(callback_invoked);
}

TEST_F(ClientManagerTest, testAck) {
  bool completed = false;
  absl::Mutex mtx;
  absl::CondVar cv;

  auto mock_ack = [&](const AckMessageRequest& request, InvocationContext<AckMessageResponse>* invocation_context) {
    absl::MutexLock lk(&mtx);
    completed = true;
    cv.SignalAll();
    invocation_context->onCompletion(true);
  };

  EXPECT_CALL(*rpc_client_, asyncAck).Times(testing::AtLeast(1)).WillRepeatedly(testing::Invoke(mock_ack));
  AckMessageRequest request;
  bool callback_invoked = false;
  auto callback = [&](const std::error_code& ec) { callback_invoked = true; };

  client_manager_->ack(target_host_, metadata_, request, absl::ToChronoMilliseconds(io_timeout_), callback);

  {
    absl::MutexLock lk(&mtx);
    if (!completed) {
      cv.WaitWithDeadline(&mtx, absl::Now() + absl::Seconds(3));
    }
  }
  EXPECT_TRUE(completed);
  EXPECT_TRUE(callback_invoked);
}

TEST_F(ClientManagerTest, testForwardMessageToDeadLetterQueue) {
  bool completed = false;
  absl::Mutex mtx;
  absl::CondVar cv;

  auto mock_forward = [&](const ForwardMessageToDeadLetterQueueRequest& request,
                          InvocationContext<ForwardMessageToDeadLetterQueueResponse>* invocation_context) {
    absl::MutexLock lk(&mtx);
    completed = true;
    cv.SignalAll();
    invocation_context->onCompletion(true);
  };

  EXPECT_CALL(*rpc_client_, asyncForwardMessageToDeadLetterQueue)
      .Times(testing::AtLeast(1))
      .WillRepeatedly(testing::Invoke(mock_forward));
  ForwardMessageToDeadLetterQueueRequest request;
  bool callback_invoked = false;
  auto callback = [&](const std::error_code& ec) { callback_invoked = true; };

  client_manager_->forwardMessageToDeadLetterQueue(target_host_, metadata_, request,
                                                   absl::ToChronoMilliseconds(io_timeout_), callback);
  {
    absl::MutexLock lk(&mtx);
    if (!completed) {
      cv.WaitWithDeadline(&mtx, absl::Now() + absl::Seconds(3));
    }
  }
  EXPECT_TRUE(completed);
  EXPECT_TRUE(callback_invoked);
}

TEST_F(ClientManagerTest, testMultiplexingCall) {
}

TEST_F(ClientManagerTest, testEndTransaction) {
  bool completed = false;
  absl::Mutex mtx;
  absl::CondVar cv;

  auto mock_end_transaction = [&](const EndTransactionRequest& request,
                                  InvocationContext<EndTransactionResponse>* invocation_context) {
    absl::MutexLock lk(&mtx);
    completed = true;
    cv.SignalAll();
    invocation_context->onCompletion(true);
  };

  EXPECT_CALL(*rpc_client_, asyncEndTransaction)
      .Times(testing::AtLeast(1))
      .WillRepeatedly(testing::Invoke(mock_end_transaction));
  EndTransactionRequest request;
  bool callback_invoked = false;
  auto callback = [&](const std::error_code& ec, const EndTransactionResponse& response) { callback_invoked = true; };

  client_manager_->endTransaction(target_host_, metadata_, request, absl::ToChronoMilliseconds(io_timeout_), callback);
  {
    absl::MutexLock lk(&mtx);
    if (!completed) {
      cv.WaitWithDeadline(&mtx, absl::Now() + absl::Seconds(3));
    }
  }
  EXPECT_TRUE(completed);
  EXPECT_TRUE(callback_invoked);
}

TEST_F(ClientManagerTest, sendSuccessTest) {
  bool completed = false;
  absl::Mutex mtx;
  absl::CondVar cv;
  SendResult captured_result;

  auto mock_send = [&](const SendMessageRequest& request, InvocationContext<SendMessageResponse>* invocation_context) {
    auto* entry = invocation_context->response.add_entries();
    entry->set_message_id("msg-id-001");
    entry->set_transaction_id("txn-id-001");
    entry->set_recall_handle("recall-handle-001");
    invocation_context->response.mutable_status()->set_code(rmq::Code::OK);
    invocation_context->onCompletion(true);
  };

  EXPECT_CALL(*rpc_client_, asyncSend).Times(1).WillOnce(testing::Invoke(mock_send));

  SendMessageRequest request;
  auto* msg = request.add_messages();
  msg->mutable_topic()->set_name(topic_);
  msg->set_body(message_body_);

  auto callback = [&](const SendResult& result) {
    absl::MutexLock lk(&mtx);
    completed = true;
    captured_result = result;
    cv.SignalAll();
  };

  client_manager_->send(target_host_, metadata_, request, std::chrono::seconds(3), callback);

  {
    absl::MutexLock lk(&mtx);
    if (!completed) {
      cv.WaitWithDeadline(&mtx, absl::Now() + absl::Seconds(3));
    }
  }

  EXPECT_TRUE(completed);
  EXPECT_FALSE(captured_result.ec);
  EXPECT_EQ("msg-id-001", captured_result.message_id);
  EXPECT_EQ("txn-id-001", captured_result.transaction_id);
  EXPECT_EQ("recall-handle-001", captured_result.recall_handle);
  EXPECT_EQ(target_host_, captured_result.target);
}

TEST_F(ClientManagerTest, sendReturnsErrorOnBadRequestTest) {
  bool completed = false;
  absl::Mutex mtx;
  absl::CondVar cv;
  SendResult captured_result;

  auto mock_send = [&](const SendMessageRequest& request, InvocationContext<SendMessageResponse>* invocation_context) {
    invocation_context->response.mutable_status()->set_code(rmq::Code::ILLEGAL_TOPIC);
    invocation_context->response.mutable_status()->set_message("Illegal topic");
    invocation_context->onCompletion(true);
  };

  EXPECT_CALL(*rpc_client_, asyncSend).Times(1).WillOnce(testing::Invoke(mock_send));

  SendMessageRequest request;
  auto* msg = request.add_messages();
  msg->mutable_topic()->set_name(topic_);
  msg->set_body(message_body_);

  auto callback = [&](const SendResult& result) {
    absl::MutexLock lk(&mtx);
    completed = true;
    captured_result = result;
    cv.SignalAll();
  };

  client_manager_->send(target_host_, metadata_, request, std::chrono::seconds(3), callback);

  {
    absl::MutexLock lk(&mtx);
    if (!completed) {
      cv.WaitWithDeadline(&mtx, absl::Now() + absl::Seconds(3));
    }
  }

  EXPECT_TRUE(completed);
  EXPECT_TRUE(static_cast<bool>(captured_result.ec));
  EXPECT_EQ(ErrorCode::IllegalTopic, captured_result.ec);
  EXPECT_TRUE(captured_result.message_id.empty());
}

TEST_F(ClientManagerTest, cleanOfflineRpcClientsRemovesDeadChannelsTest) {
  std::string live_host = "ipv4:10.0.0.1:10911";
  std::string dead_host = "ipv4:10.0.0.2:10911";

  auto live_rpc_client = std::make_shared<testing::NiceMock<RpcClientMock>>();
  ON_CALL(*live_rpc_client, ok).WillByDefault(testing::Return(true));
  ON_CALL(*live_rpc_client, needHeartbeat()).WillByDefault(testing::Return(false));

  auto dead_rpc_client = std::make_shared<testing::NiceMock<RpcClientMock>>();
  ON_CALL(*dead_rpc_client, ok).WillByDefault(testing::Return(false));
  ON_CALL(*dead_rpc_client, needHeartbeat()).WillByDefault(testing::Return(false));

  client_manager_->addRpcClient(live_host, live_rpc_client);
  client_manager_->addRpcClient(dead_host, dead_rpc_client);

  std::vector<std::string> removed = client_manager_->cleanOfflineRpcClients();

  // Dead channel should be removed; live channel should be retained.
  EXPECT_EQ(1u, removed.size());
  EXPECT_EQ(dead_host, removed[0]);

  // Live host's mock should still be returned from the map.
  auto live_client = client_manager_->getRpcClient(live_host);
  EXPECT_EQ(live_rpc_client, live_client);

  // Dead host's mock was removed; getRpcClient creates a new real client instead.
  auto dead_client = client_manager_->getRpcClient(dead_host);
  EXPECT_NE(dead_rpc_client, dead_client);
}

TEST_F(ClientManagerTest, stateReturnsStartedTest) {
  EXPECT_EQ(State::STARTED, client_manager_->state());
}

ROCKETMQ_NAMESPACE_END