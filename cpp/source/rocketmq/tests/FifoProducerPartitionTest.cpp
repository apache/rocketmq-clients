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
#include <chrono>
#include <memory>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include "ClientManagerMock.h"
#include "FifoContext.h"
#include "FifoProducerPartition.h"
#include "MixAll.h"
#include "NameServerResolverMock.h"
#include "ProducerImpl.h"
#include "RpcClientMock.h"
#include "SendResult.h"
#include "TelemetryBidiReactor.h"
#include "TopicRouteData.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "rocketmq/Message.h"
#include "rocketmq/SendCallback.h"
#include "rocketmq/SendReceipt.h"

#include "absl/synchronization/mutex.h"
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

class FifoProducerPartitionTest : public testing::Test {
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

    partition_ = std::make_shared<FifoProducerPartition>(producer_, std::string("test-partition"));
  }

  void TearDown() override {
    // Wait briefly for pending async operations to complete
    absl::SleepFor(absl::Milliseconds(200));
    // Reset partition first
    partition_.reset();
    // Reset all shared_ptrs to ensure clean destruction order
    producer_.reset();
    rpc_client_.reset();
    client_manager_.reset();
    resolver_mock_.reset();
  }

  void installSendSuccessHandler() {
    ON_CALL(*client_manager_, send)
        .WillByDefault(testing::Invoke(
            [this](const std::string&, const Metadata&, SendMessageRequest&, std::chrono::milliseconds, SendResultCallback cb) {
              {
                absl::MutexLock lk(&mtx_);
                send_count_++;
                cv_.SignalAll();
              }
              // Dispatch callback asynchronously to avoid FifoProducerPartition mutex deadlock
              std::thread([this, cb]() {
                SendResult result;
                result.message_id = "msg-id";
                cb(result);
                // Signal after callback completes so waitForResults can detect it
                cv_.SignalAll();
              }).detach();
              return true;
            }));
  }

  SendCallback makeResultCallback() {
    return [this](const std::error_code& ec, SendReceipt&&) {
      absl::MutexLock lk(&mtx_);
      results_.push_back(ec);
      cv_.SignalAll();
    };
  }

  MessageConstPtr makeMessage(const std::string& body = "test-body") {
    return Message::newBuilder().withTopic("test-topic").withBody(body).build();
  }

  void waitForSends(int expected) {
    absl::MutexLock lk(&mtx_);
    auto deadline = absl::Now() + absl::Seconds(3);
    while (send_count_ < expected) {
      if (cv_.WaitWithDeadline(&mtx_, deadline)) {
        break;  // timeout
      }
    }
  }

  void waitForResults(int expected) {
    absl::MutexLock lk(&mtx_);
    auto deadline = absl::Now() + absl::Seconds(3);
    while (static_cast<int>(results_.size()) < expected) {
      if (cv_.WaitWithDeadline(&mtx_, deadline)) {
        break;  // timeout
      }
    }
  }

  std::shared_ptr<ProducerImpl> producer_;
  std::shared_ptr<testing::NiceMock<NameServerResolverMock>> resolver_mock_;
  std::shared_ptr<testing::NiceMock<ClientManagerMock>> client_manager_;
  std::shared_ptr<TestRpcClient> rpc_client_;
  std::shared_ptr<FifoProducerPartition> partition_;

  absl::Mutex mtx_;
  absl::CondVar cv_;
  int send_count_ GUARDED_BY(mtx_) = 0;
  std::vector<std::error_code> results_;
};

TEST_F(FifoProducerPartitionTest, addTriggersSendTest) {
  installSendSuccessHandler();

  auto msg = makeMessage();
  SendCallback cb = makeResultCallback();
  FifoContext ctx(std::move(msg), std::move(cb));

  partition_->add(std::move(ctx));
  waitForSends(1);
  waitForResults(1);

  absl::MutexLock lk(&mtx_);
  EXPECT_EQ(1, send_count_);
  ASSERT_EQ(1u, results_.size());
  EXPECT_FALSE(results_[0]);
}

TEST_F(FifoProducerPartitionTest, multipleMessagesPreserveOrderTest) {
  std::vector<std::string> sent_bodies;

  ON_CALL(*client_manager_, send)
      .WillByDefault(testing::Invoke(
          [this, &sent_bodies](const std::string&, const Metadata&, SendMessageRequest& request,
                               std::chrono::milliseconds, SendResultCallback cb) {
            {
              absl::MutexLock lk(&mtx_);
              if (request.messages_size() > 0) {
                sent_bodies.push_back(request.messages(0).body());
              }
              send_count_++;
              cv_.SignalAll();
            }
            std::thread([cb]() {
              SendResult result;
              result.message_id = "msg-id";
              cb(result);
            }).detach();
            return true;
          }));

  const int num_messages = 5;
  for (int i = 0; i < num_messages; i++) {
    auto msg = makeMessage("msg-" + std::to_string(i));
    SendCallback cb = makeResultCallback();
    FifoContext ctx(std::move(msg), std::move(cb));
    partition_->add(std::move(ctx));
  }

  waitForSends(num_messages);
  waitForResults(num_messages);

  absl::MutexLock lk(&mtx_);
  EXPECT_EQ(num_messages, send_count_);
  ASSERT_EQ(static_cast<size_t>(num_messages), results_.size());
  for (int i = 0; i < num_messages; i++) {
    EXPECT_FALSE(results_[i]);
  }

  // Verify messages were sent in FIFO order
  ASSERT_EQ(static_cast<size_t>(num_messages), sent_bodies.size());
  for (int i = 0; i < num_messages; i++) {
    EXPECT_EQ("msg-" + std::to_string(i), sent_bodies[i])
        << "Message at index " << i << " was out of order";
  }
}

TEST_F(FifoProducerPartitionTest, onCompleteSuccessInvokesUserCallbackTest) {
  bool callback_invoked = false;
  std::error_code received_ec;
  SendCallback user_cb = [&callback_invoked, &received_ec](const std::error_code& ec, SendReceipt&&) {
    callback_invoked = true;
    received_ec = ec;
  };

  SendReceipt receipt;
  receipt.message = makeMessage();
  receipt.message_id = "test-id";
  std::error_code ec;

  partition_->onComplete(ec, std::move(receipt), user_cb);

  EXPECT_TRUE(callback_invoked);
  EXPECT_FALSE(received_ec);
}

TEST_F(FifoProducerPartitionTest, onCompleteFailureRequeuesMessageTest) {
  bool callback_invoked = false;
  SendCallback user_cb = [&callback_invoked](const std::error_code&, SendReceipt&&) {
    callback_invoked = true;
  };

  SendReceipt receipt;
  receipt.message = makeMessage("retry-message");
  std::error_code ec = std::make_error_code(std::errc::io_error);

  partition_->onComplete(ec, std::move(receipt), user_cb);

  // On failure, user callback should NOT be invoked (message is requeued instead)
  EXPECT_FALSE(callback_invoked);
}

TEST_F(FifoProducerPartitionTest, failedMessageRetriedViaOnCompleteTest) {
  // Install handler that fails once then succeeds
  std::atomic<int> attempt{0};
  ON_CALL(*client_manager_, send)
      .WillByDefault(testing::Invoke(
          [this, &attempt](const std::string&, const Metadata&, SendMessageRequest&, std::chrono::milliseconds, SendResultCallback cb) {
            int current = attempt.fetch_add(1);
            {
              absl::MutexLock lk(&mtx_);
              send_count_++;
              cv_.SignalAll();
            }
            std::thread([cb, current]() {
              SendResult result;
              if (current == 0) {
                result.ec = std::make_error_code(std::errc::io_error);
              }
              result.message_id = "msg-id";
              cb(result);
            }).detach();
            return true;
          }));

  // Add a message - first attempt fails, FifoProducerPartition retries
  auto msg = makeMessage("retry-msg");
  SendCallback cb = makeResultCallback();
  FifoContext ctx(std::move(msg), std::move(cb));
  partition_->add(std::move(ctx));

  // Wait for 2 send attempts (1 failure + 1 success)
  waitForSends(2);
  waitForResults(1);

  absl::MutexLock lk(&mtx_);
  EXPECT_GE(send_count_, 2);
  ASSERT_GE(results_.size(), 1u);
  EXPECT_FALSE(results_.back());
}

TEST_F(FifoProducerPartitionTest, emptyPartitionTrySendDoesNotCrashTest) {
  installSendSuccessHandler();
  partition_->trySend();

  absl::MutexLock lk(&mtx_);
  EXPECT_EQ(0, send_count_);
}

TEST_F(FifoProducerPartitionTest, fifoContextMoveConstructionTest) {
  auto msg = makeMessage("move-test");
  SendCallback cb = makeResultCallback();
  FifoContext ctx1(std::move(msg), std::move(cb));

  EXPECT_NE(nullptr, ctx1.message);
  EXPECT_TRUE(ctx1.callback != nullptr);

  FifoContext ctx2(std::move(ctx1));
  EXPECT_NE(nullptr, ctx2.message);
  EXPECT_TRUE(ctx2.callback != nullptr);
  EXPECT_EQ(nullptr, ctx1.message);
}

ROCKETMQ_NAMESPACE_END
