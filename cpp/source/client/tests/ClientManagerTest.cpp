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

#include <memory>
#include <system_error>

#include "ClientManagerImpl.h"
#include "RpcClientMock.h"
#include "gtest/gtest.h"

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

ROCKETMQ_NAMESPACE_END