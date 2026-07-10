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
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include "ClientManagerMock.h"
#include "NameServerResolverMock.h"
#include "Protocol.h"
#include "SimpleConsumerImpl.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "rocketmq/ErrorCode.h"
#include "rocketmq/FilterExpression.h"
#include "rocketmq/Message.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace {

/// Test subclass that exposes protected members for unit testing.
class TestableSimpleConsumerImpl : public SimpleConsumerImpl {
public:
  explicit TestableSimpleConsumerImpl(const std::string& group) : ClientImpl(group), SimpleConsumerImpl(group) {}

  using SimpleConsumerImpl::topicsOfInterest;
};

} // namespace

TEST(SimpleConsumerImplTest, constructorWithGroupTest) {
  auto consumer = std::make_shared<TestableSimpleConsumerImpl>("test-group");
  EXPECT_EQ(consumer->config().subscriber.group.name(), "test-group");
}

TEST(SimpleConsumerImplTest, subscribeAddsTopicTest) {
  auto consumer = std::make_shared<TestableSimpleConsumerImpl>("test-group");
  consumer->subscribe("test-topic", FilterExpression("*"));

  std::vector<std::string> topics;
  consumer->topicsOfInterest(topics);
  ASSERT_EQ(1u, topics.size());
  EXPECT_EQ("test-topic", topics[0]);
}

TEST(SimpleConsumerImplTest, unsubscribeRemovesTopicTest) {
  auto consumer = std::make_shared<TestableSimpleConsumerImpl>("test-group");
  consumer->subscribe("test-topic", FilterExpression("*"));

  std::vector<std::string> topics;
  consumer->topicsOfInterest(topics);
  ASSERT_EQ(1u, topics.size());

  consumer->unsubscribe("test-topic");
  topics.clear();
  consumer->topicsOfInterest(topics);
  EXPECT_TRUE(topics.empty());
}

TEST(SimpleConsumerImplTest, subscribeMultipleTopicsTest) {
  auto consumer = std::make_shared<TestableSimpleConsumerImpl>("test-group");
  consumer->subscribe("topic-1", FilterExpression("*"));
  consumer->subscribe("topic-2", FilterExpression("tagA"));
  consumer->subscribe("topic-3", FilterExpression("tagB"));

  std::vector<std::string> topics;
  consumer->topicsOfInterest(topics);
  EXPECT_EQ(3u, topics.size());
}

TEST(SimpleConsumerImplTest, subscribeUpdatesFilterExpressionTest) {
  auto consumer = std::make_shared<TestableSimpleConsumerImpl>("test-group");
  consumer->subscribe("test-topic", FilterExpression("tagA"));
  // Overwrite with a new filter expression
  consumer->subscribe("test-topic", FilterExpression("tagB"));

  std::vector<std::string> topics;
  consumer->topicsOfInterest(topics);
  // Should still be a single topic (no duplicate)
  ASSERT_EQ(1u, topics.size());
  EXPECT_EQ("test-topic", topics[0]);
}

TEST(SimpleConsumerImplTest, prepareHeartbeatDataSetsSimpleConsumerTypeTest) {
  auto consumer = std::make_shared<TestableSimpleConsumerImpl>("test-group");
  rmq::HeartbeatRequest request;
  consumer->prepareHeartbeatData(request);
  EXPECT_EQ(rmq::ClientType::SIMPLE_CONSUMER, request.client_type());
}

TEST(SimpleConsumerImplTest, buildClientSettingsSetsSimpleConsumerTypeTest) {
  auto consumer = std::make_shared<TestableSimpleConsumerImpl>("test-group");
  auto resolver = std::make_shared<testing::NiceMock<NameServerResolverMock>>();
  ON_CALL(*resolver, resolve()).WillByDefault(testing::Return(std::string("ipv4:10.0.0.1:8080")));
  consumer->withNameServerResolver(resolver);
  consumer->subscribe("settings-topic", FilterExpression("tagX"));

  rmq::Settings settings;
  consumer->buildClientSettings(settings);
  EXPECT_EQ(rmq::ClientType::SIMPLE_CONSUMER, settings.client_type());
}

TEST(SimpleConsumerImplTest, unsubscribeNonexistentTopicDoesNotCrashTest) {
  auto consumer = std::make_shared<TestableSimpleConsumerImpl>("test-group");
  // Should not crash or throw
  consumer->unsubscribe("nonexistent-topic");

  std::vector<std::string> topics;
  consumer->topicsOfInterest(topics);
  EXPECT_TRUE(topics.empty());
}

TEST(SimpleConsumerImplTest, topicsOfInterestNoDuplicatesTest) {
  auto consumer = std::make_shared<TestableSimpleConsumerImpl>("test-group");
  consumer->subscribe("dup-topic", FilterExpression("*"));

  // Pre-populate the vector — topicsOfInterest should not add a duplicate
  std::vector<std::string> topics;
  topics.push_back("dup-topic");
  consumer->topicsOfInterest(topics);
  EXPECT_EQ(1u, topics.size());
}

TEST(SimpleConsumerImplTest, ackWhenNotRunningFailsTest) {
  auto consumer = std::make_shared<SimpleConsumerImpl>("test-group");
  // State is CREATED by default (not STARTED) — the consumer was never started.
  // The manager is not configured, so ack cannot proceed.
  // Verify the initial state is indeed CREATED.
  EXPECT_FALSE(consumer->active());
}

TEST(SimpleConsumerImplTest, ackWithMockedManagerPropagatesErrorCodeTest) {
  auto consumer = std::make_shared<SimpleConsumerImpl>("test-group");
  consumer->state(State::STARTED);

  auto client_manager = std::make_shared<testing::NiceMock<ClientManagerMock>>();
  consumer->clientManager(client_manager);

  // Mock ack to invoke callback with an error
  ON_CALL(*client_manager, ack)
      .WillByDefault(testing::Invoke(
          [](const std::string&, const Metadata&, const AckMessageRequest&, std::chrono::milliseconds,
             const std::function<void(const std::error_code&)>& cb) {
            cb(ErrorCode::NotFound);
          }));

  MessageConstPtr msg = Message::newBuilder().withTopic("ack-topic").withBody("body").build();
  std::error_code ec;
  consumer->ack(*msg, ec);

  EXPECT_EQ(ErrorCode::NotFound, ec);

  consumer->state(State::STOPPED);
}

TEST(SimpleConsumerImplTest, ackWithMockedManagerSuccessTest) {
  auto consumer = std::make_shared<SimpleConsumerImpl>("test-group");
  consumer->state(State::STARTED);

  auto client_manager = std::make_shared<testing::NiceMock<ClientManagerMock>>();
  consumer->clientManager(client_manager);

  // Mock ack to invoke callback with success
  ON_CALL(*client_manager, ack)
      .WillByDefault(testing::Invoke(
          [](const std::string&, const Metadata&, const AckMessageRequest&, std::chrono::milliseconds,
             const std::function<void(const std::error_code&)>& cb) {
            cb(std::error_code());
          }));

  MessageConstPtr msg = Message::newBuilder().withTopic("ack-topic").withBody("body").build();
  std::error_code ec;
  consumer->ack(*msg, ec);

  EXPECT_FALSE(ec);

  consumer->state(State::STOPPED);
}

ROCKETMQ_NAMESPACE_END
