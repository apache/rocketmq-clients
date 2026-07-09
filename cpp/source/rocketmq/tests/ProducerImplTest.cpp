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
#include <atomic>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "ClientManagerMock.h"
#include "MixAll.h"
#include "NameServerResolverMock.h"
#include "ProducerImpl.h"
#include "RpcClientMock.h"
#include "SendResult.h"
#include "TelemetryBidiReactor.h"
#include "TopicRouteData.h"
#include "TransactionImpl.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "rocketmq/ErrorCode.h"
#include "rocketmq/Message.h"
#include "rocketmq/SendCallback.h"
#include "rocketmq/SendReceipt.h"

#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "grpcpp/create_channel.h"
#include "grpcpp/security/credentials.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace {

rmq::MessageQueue createMessageQueue(const std::string& topic, int id, const std::string& broker_name) {
  rmq::MessageQueue mq;
  mq.mutable_topic()->set_name(topic);
  mq.set_id(id);
  mq.mutable_broker()->set_name(broker_name);
  mq.mutable_broker()->set_id(MixAll::MASTER_BROKER_ID);
  mq.mutable_broker()->mutable_endpoints()->set_scheme(rmq::AddressScheme::IPv4);
  auto* addr = mq.mutable_broker()->mutable_endpoints()->add_addresses();
  addr->set_host("127.0.0.1");
  addr->set_port(19999);
  mq.set_permission(rmq::Permission::READ_WRITE);
  return mq;
}

TopicRouteDataPtr createRouteData(const std::string& topic, int num_queues = 1) {
  std::vector<rmq::MessageQueue> queues;
  for (int i = 0; i < num_queues; i++) {
    queues.push_back(createMessageQueue(topic, i, "broker-0"));
  }
  return std::make_shared<TopicRouteData>(queues);
}

/// A minimal RpcClient mock that supports asyncTelemetry with a real stub
class TestRpcClient : public testing::NiceMock<RpcClientMock> {
public:
  TestRpcClient() {
    channel_ = grpc::CreateChannel("127.0.0.1:19999", grpc::InsecureChannelCredentials());
    stub_ = rmq::MessagingService::NewStub(channel_);
    static const std::string addr = "127.0.0.1:19999";
    ON_CALL(*this, remoteAddress).WillByDefault(testing::ReturnRef(addr));
  }

  std::shared_ptr<TelemetryBidiReactor> asyncTelemetry(std::weak_ptr<Client> /*client*/) override {
    // Pass an expired weak_ptr so TelemetryBidiReactor short-circuits without initiating gRPC calls
    std::shared_ptr<Client> expired;
    std::weak_ptr<Client> weak_expired(expired);
    return std::make_shared<TelemetryBidiReactor>(weak_expired, stub_.get(), "127.0.0.1:19999");
  }

private:
  std::shared_ptr<grpc::Channel> channel_;
  std::unique_ptr<rmq::MessagingService::Stub> stub_;
};

}  // namespace

class ProducerImplTest : public testing::Test {
protected:
  void SetUp() override {
    producer_ = std::make_shared<ProducerImpl>();
    producer_->state(State::STARTED);
    producer_->maxAttemptTimes(1);

    resolver_mock_ = std::make_shared<testing::NiceMock<NameServerResolverMock>>();
    ON_CALL(*resolver_mock_, resolve).WillByDefault(testing::Return(std::string("127.0.0.1:9876")));
    producer_->withNameServerResolver(resolver_mock_);

    client_manager_ = std::make_shared<testing::NiceMock<ClientManagerMock>>();
    producer_->clientManager(client_manager_);

    // Mock getRpcClient to return a test RpcClient that supports telemetry
    rpc_client_ = std::make_shared<TestRpcClient>();
    ON_CALL(*client_manager_, getRpcClient)
        .WillByDefault(testing::Return(rpc_client_));

    // Mock resolveRoute to return valid route data
    ON_CALL(*client_manager_, resolveRoute)
        .WillByDefault(testing::Invoke(
            [](const std::string&, const Metadata&, const QueryRouteRequest& request,
               std::chrono::milliseconds,
               const std::function<void(const std::error_code&, const TopicRouteDataPtr&)>& cb) {
              std::string topic = request.topic().name();
              std::error_code ec;
              cb(ec, createRouteData(topic));
            }));
  }

  void TearDown() override {
    // Wait briefly for pending async operations to complete
    absl::SleepFor(absl::Milliseconds(200));
    producer_.reset();
    rpc_client_.reset();
    client_manager_.reset();
    resolver_mock_.reset();
  }

  void installSendSuccessHandler() {
    ON_CALL(*client_manager_, send)
        .WillByDefault(testing::Invoke(
            [this](const std::string&, const Metadata&, SendMessageRequest&, SendResultCallback cb) {
              {
                absl::MutexLock lk(&mtx_);
                send_count_++;
                cv_.SignalAll();
              }
              // Dispatch callback asynchronously to avoid deadlock
              std::thread([this, cb]() {
                SendResult result;
                result.message_id = "msg-id";
                cb(result);
                cv_.SignalAll();
              }).detach();
              return true;
            }));
  }

  MessageConstPtr makeMessage(const std::string& topic = "test-topic",
                              const std::string& body = "test-body") {
    return Message::newBuilder().withTopic(topic).withBody(body).build();
  }

  std::shared_ptr<ProducerImpl> producer_;
  std::shared_ptr<testing::NiceMock<NameServerResolverMock>> resolver_mock_;
  std::shared_ptr<testing::NiceMock<ClientManagerMock>> client_manager_;
  std::shared_ptr<TestRpcClient> rpc_client_;

  absl::Mutex mtx_;
  absl::CondVar cv_;
  int send_count_ GUARDED_BY(mtx_) = 0;
};

// Test 1: Sending a message with an empty body should fail with MessageBodyEmpty
TEST_F(ProducerImplTest, validateEmptyBodyFailsTest) {
  installSendSuccessHandler();

  auto msg = Message::newBuilder().withTopic("test-topic").withBody("").build();
  std::error_code ec;
  producer_->send(std::move(msg), ec);

  EXPECT_EQ(ErrorCode::MessageBodyEmpty, ec);
}

// Test 2: Sending a message with an empty topic should fail with IllegalTopic
TEST_F(ProducerImplTest, validateEmptyTopicFailsTest) {
  installSendSuccessHandler();

  // Build message with empty topic — body is non-empty so that validation
  // reaches the topic check without short-circuiting on empty body first.
  auto msg = Message::newBuilder().withTopic("").withBody("hello").build();
  std::error_code ec;
  producer_->send(std::move(msg), ec);

  EXPECT_EQ(ErrorCode::IllegalTopic, ec);
}

// Test 3: Sending a message whose body exceeds max_body_size should fail with PayloadTooLarge
TEST_F(ProducerImplTest, validateOversizedBodyFailsTest) {
  installSendSuccessHandler();

  // Lower the threshold so we can test with a small oversized body
  producer_->config().publisher.max_body_size = 100;

  std::string oversized_body(200, 'x');
  auto msg = Message::newBuilder().withTopic("test-topic").withBody(oversized_body).build();
  std::error_code ec;
  producer_->send(std::move(msg), ec);

  EXPECT_EQ(ErrorCode::PayloadTooLarge, ec);
}

// Test 4: Sending a normal message should pass validation and succeed
TEST_F(ProducerImplTest, validateNormalMessagePassesTest) {
  installSendSuccessHandler();

  auto msg = makeMessage();
  std::error_code ec;
  SendReceipt receipt = producer_->send(std::move(msg), ec);

  EXPECT_FALSE(ec) << "Expected no error, got: " << ec.message();
  EXPECT_EQ("msg-id", receipt.message_id);
}

// Test 5: Isolating an endpoint and then checking isolation status
TEST_F(ProducerImplTest, isolateEndpointAndCheckTest) {
  const std::string endpoint = "127.0.0.1:19999";

  EXPECT_FALSE(producer_->isEndpointIsolated(endpoint));

  producer_->isolateEndpoint(endpoint);

  EXPECT_TRUE(producer_->isEndpointIsolated(endpoint));
  EXPECT_FALSE(producer_->isEndpointIsolated("10.0.0.1:19999"));
}

// Test 6: Sending when producer is not in STARTED state should return IllegalState
TEST_F(ProducerImplTest, sendSyncWhenNotRunningReturnsIllegalStateTest) {
  // Reset state to CREATED (not STARTED)
  producer_->state(State::CREATED);

  auto msg = makeMessage();
  std::error_code ec;
  producer_->send(std::move(msg), ec);

  EXPECT_EQ(ErrorCode::IllegalState, ec);
}

// Test 7: Transactional send should reject FIFO messages (with group set)
TEST_F(ProducerImplTest, transactionalSendRejectsFifoMessageTest) {
  auto transaction = producer_->beginTransaction();

  // Build a message with group set (FIFO message)
  auto msg = Message::newBuilder()
                 .withTopic("test-topic")
                 .withBody("test-body")
                 .withGroup("test-group")
                 .build();

  std::error_code ec;
  producer_->send(std::move(msg), ec, *transaction);

  EXPECT_EQ(ErrorCode::MessagePropertyConflictWithType, ec);
}

// Test 8: prepareHeartbeatData should set client_type to PRODUCER
TEST_F(ProducerImplTest, prepareHeartbeatDataSetsProducerTypeTest) {
  HeartbeatRequest request;
  producer_->prepareHeartbeatData(request);

  EXPECT_EQ(rmq::ClientType::PRODUCER, request.client_type());
}

// Test 9: topicsOfInterest should return topics configured via withTopics
TEST_F(ProducerImplTest, topicsOfInterestReturnsConfiguredTopicsTest) {
  std::vector<std::string> input_topics = {"topic-a", "topic-b", "topic-c"};
  producer_->withTopics(input_topics);

  std::vector<std::string> result_topics;
  producer_->topicsOfInterest(result_topics);

  ASSERT_EQ(3u, result_topics.size());
  EXPECT_EQ("topic-a", result_topics[0]);
  EXPECT_EQ("topic-b", result_topics[1]);
  EXPECT_EQ("topic-c", result_topics[2]);
}

ROCKETMQ_NAMESPACE_END
