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
#include <algorithm>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "ClientManagerMock.h"
#include "MixAll.h"
#include "Protocol.h"
#include "PushConsumerImpl.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "rocketmq/ConsumeResult.h"
#include "rocketmq/ExpressionType.h"
#include "rocketmq/FilterExpression.h"
#include "rocketmq/Message.h"
#include "rocketmq/MessageListener.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace {

std::shared_ptr<PushConsumerImpl> createConsumer(const std::string& group = "test-group") {
  auto consumer = std::make_shared<PushConsumerImpl>(group);
  auto client_manager = std::make_shared<testing::NiceMock<ClientManagerMock>>();
  consumer->clientManager(client_manager);
  return consumer;
}

} // namespace

TEST(PushConsumerImplTest, subscribeTest) {
  auto consumer = createConsumer();
  consumer->subscribe("test-topic", "tagA", ExpressionType::TAG);

  auto filter = consumer->getFilterExpression("test-topic");
  ASSERT_TRUE(filter.has_value());
  EXPECT_EQ("tagA", filter->content_);
  EXPECT_EQ(ExpressionType::TAG, filter->type_);
}

TEST(PushConsumerImplTest, subscribeWithSQL92Test) {
  auto consumer = createConsumer();
  consumer->subscribe("sql-topic", "color = 'red'", ExpressionType::SQL92);

  auto filter = consumer->getFilterExpression("sql-topic");
  ASSERT_TRUE(filter.has_value());
  EXPECT_EQ("color = 'red'", filter->content_);
  EXPECT_EQ(ExpressionType::SQL92, filter->type_);
}

TEST(PushConsumerImplTest, subscribeDefaultExpressionTypeTest) {
  auto consumer = createConsumer();
  consumer->subscribe("default-type-topic", "*");

  auto filter = consumer->getFilterExpression("default-type-topic");
  ASSERT_TRUE(filter.has_value());
  EXPECT_EQ("*", filter->content_);
  EXPECT_EQ(ExpressionType::TAG, filter->type_);
}

TEST(PushConsumerImplTest, subscribeEmptyTagBecomesWildcardTest) {
  auto consumer = createConsumer();
  consumer->subscribe("wildcard-topic", "", ExpressionType::TAG);

  auto filter = consumer->getFilterExpression("wildcard-topic");
  ASSERT_TRUE(filter.has_value());
  EXPECT_EQ(FilterExpression::WILD_CARD_TAG, filter->content_);
}

TEST(PushConsumerImplTest, unsubscribeTest) {
  auto consumer = createConsumer();
  consumer->subscribe("topic-to-remove", "tagA");

  auto filter = consumer->getFilterExpression("topic-to-remove");
  ASSERT_TRUE(filter.has_value());

  consumer->unsubscribe("topic-to-remove");

  filter = consumer->getFilterExpression("topic-to-remove");
  EXPECT_FALSE(filter.has_value());
}

TEST(PushConsumerImplTest, unsubscribeNonExistentTopicTest) {
  auto consumer = createConsumer();
  // Should not throw or crash
  consumer->unsubscribe("non-existent-topic");

  auto filter = consumer->getFilterExpression("non-existent-topic");
  EXPECT_FALSE(filter.has_value());
}

TEST(PushConsumerImplTest, subscribeMultipleTopicsTest) {
  auto consumer = createConsumer();
  consumer->subscribe("topic-1", "tagA");
  consumer->subscribe("topic-2", "tagB");
  consumer->subscribe("topic-3", "tagC");

  auto filter1 = consumer->getFilterExpression("topic-1");
  auto filter2 = consumer->getFilterExpression("topic-2");
  auto filter3 = consumer->getFilterExpression("topic-3");

  ASSERT_TRUE(filter1.has_value());
  ASSERT_TRUE(filter2.has_value());
  ASSERT_TRUE(filter3.has_value());

  EXPECT_EQ("tagA", filter1->content_);
  EXPECT_EQ("tagB", filter2->content_);
  EXPECT_EQ("tagC", filter3->content_);
}

TEST(PushConsumerImplTest, subscribeOverwritesExistingFilterTest) {
  auto consumer = createConsumer();
  consumer->subscribe("topic", "tagA");
  consumer->subscribe("topic", "tagB");

  auto filter = consumer->getFilterExpression("topic");
  ASSERT_TRUE(filter.has_value());
  // emplace does not overwrite if key already exists
  // The first subscription should remain
  EXPECT_EQ("tagA", filter->content_);
}

TEST(PushConsumerImplTest, topicsOfInterestTest) {
  auto consumer = createConsumer();
  consumer->subscribe("topic-alpha", "tagA");
  consumer->subscribe("topic-beta", "tagB");
  consumer->subscribe("topic-gamma", "tagC");

  std::vector<std::string> topics;
  consumer->topicsOfInterest(topics);

  EXPECT_EQ(3u, topics.size());
  EXPECT_NE(topics.end(), std::find(topics.begin(), topics.end(), "topic-alpha"));
  EXPECT_NE(topics.end(), std::find(topics.begin(), topics.end(), "topic-beta"));
  EXPECT_NE(topics.end(), std::find(topics.begin(), topics.end(), "topic-gamma"));
}

TEST(PushConsumerImplTest, topicsOfInterestAfterUnsubscribeTest) {
  auto consumer = createConsumer();
  consumer->subscribe("topic-keep", "tagA");
  consumer->subscribe("topic-remove", "tagB");

  consumer->unsubscribe("topic-remove");

  std::vector<std::string> topics;
  consumer->topicsOfInterest(topics);

  EXPECT_EQ(1u, topics.size());
  EXPECT_EQ("topic-keep", topics[0]);
}

TEST(PushConsumerImplTest, topicsOfInterestEmptyTest) {
  auto consumer = createConsumer();
  std::vector<std::string> topics;
  consumer->topicsOfInterest(topics);
  EXPECT_TRUE(topics.empty());
}

TEST(PushConsumerImplTest, getFilterExpressionForNonExistentTopicTest) {
  auto consumer = createConsumer();
  auto filter = consumer->getFilterExpression("unknown-topic");
  EXPECT_FALSE(filter.has_value());
}

TEST(PushConsumerImplTest, prepareHeartbeatDataTest) {
  auto consumer = createConsumer();
  HeartbeatRequest request;
  consumer->prepareHeartbeatData(request);

  EXPECT_EQ(rmq::ClientType::PUSH_CONSUMER, request.client_type());
  EXPECT_EQ("test-group", request.group().name());
}

TEST(PushConsumerImplTest, buildClientSettingsTest) {
  auto consumer = createConsumer();
  consumer->subscribe("settings-topic", "tagA", ExpressionType::TAG);

  rmq::Settings settings;
  consumer->buildClientSettings(settings);

  EXPECT_EQ(rmq::ClientType::PUSH_CONSUMER, settings.client_type());
  EXPECT_EQ("test-group", settings.subscription().group().name());
  ASSERT_EQ(1, settings.subscription().subscriptions_size());
  EXPECT_EQ("settings-topic", settings.subscription().subscriptions(0).topic().name());
  EXPECT_EQ("tagA", settings.subscription().subscriptions(0).expression().expression());
  EXPECT_EQ(rmq::FilterType::TAG, settings.subscription().subscriptions(0).expression().type());
}

TEST(PushConsumerImplTest, buildClientSettingsSQL92FilterTest) {
  auto consumer = createConsumer();
  consumer->subscribe("sql-settings-topic", "price > 100", ExpressionType::SQL92);

  rmq::Settings settings;
  consumer->buildClientSettings(settings);

  EXPECT_EQ(rmq::ClientType::PUSH_CONSUMER, settings.client_type());
  ASSERT_EQ(1, settings.subscription().subscriptions_size());
  EXPECT_EQ(rmq::FilterType::SQL, settings.subscription().subscriptions(0).expression().type());
  EXPECT_EQ("price > 100", settings.subscription().subscriptions(0).expression().expression());
}

TEST(PushConsumerImplTest, buildClientSettingsMultipleSubscriptionsTest) {
  auto consumer = createConsumer();
  consumer->subscribe("topic-a", "tagA", ExpressionType::TAG);
  consumer->subscribe("topic-b", "color = 'blue'", ExpressionType::SQL92);

  rmq::Settings settings;
  consumer->buildClientSettings(settings);

  EXPECT_EQ(2, settings.subscription().subscriptions_size());
}

TEST(PushConsumerImplTest, maxDeliveryAttemptsDefaultTest) {
  auto consumer = createConsumer();
  EXPECT_EQ(MixAll::DEFAULT_MAX_DELIVERY_ATTEMPTS, consumer->maxDeliveryAttempts());
}

TEST(PushConsumerImplTest, receiveBatchSizeDefaultTest) {
  auto consumer = createConsumer();
  EXPECT_EQ(MixAll::DEFAULT_RECEIVE_MESSAGE_BATCH_SIZE, consumer->receiveBatchSize());
}

TEST(PushConsumerImplTest, consumeThreadPoolSizeDefaultTest) {
  auto consumer = createConsumer();
  EXPECT_EQ(MixAll::DEFAULT_CONSUME_THREAD_POOL_SIZE, consumer->consumeThreadPoolSize());
}

TEST(PushConsumerImplTest, consumeThreadPoolSizeSetterTest) {
  auto consumer = createConsumer();
  consumer->consumeThreadPoolSize(10);
  EXPECT_EQ(10u, consumer->consumeThreadPoolSize());
}

TEST(PushConsumerImplTest, consumeThreadPoolSizeSetterMinimumTest) {
  auto consumer = createConsumer();
  consumer->consumeThreadPoolSize(1);
  EXPECT_EQ(1u, consumer->consumeThreadPoolSize());
}

TEST(PushConsumerImplTest, consumeThreadPoolSizeSetterRejectsZeroTest) {
  auto consumer = createConsumer();
  uint32_t original = consumer->consumeThreadPoolSize();
  consumer->consumeThreadPoolSize(0);
  // 0 is less than 1, so the value should remain unchanged
  EXPECT_EQ(original, consumer->consumeThreadPoolSize());
}

TEST(PushConsumerImplTest, consumeThreadPoolSizeSetterRejectsNegativeTest) {
  auto consumer = createConsumer();
  uint32_t original = consumer->consumeThreadPoolSize();
  consumer->consumeThreadPoolSize(-1);
  // -1 is less than 1, so the value should remain unchanged
  EXPECT_EQ(original, consumer->consumeThreadPoolSize());
}

TEST(PushConsumerImplTest, registerMessageListenerTest) {
  auto consumer = createConsumer();
  bool listener_called = false;
  auto listener = [&listener_called](const Message&) {
    listener_called = true;
    return ConsumeResult::SUCCESS;
  };

  consumer->registerMessageListener(listener);

  // Verify the listener is stored by invoking it through the accessor
  auto& stored_listener = consumer->messageListener();
  ASSERT_TRUE(static_cast<bool>(stored_listener));

  auto msg = Message::newBuilder().withTopic("test-topic").build();
  stored_listener(*msg);
  EXPECT_TRUE(listener_called);
}

TEST(PushConsumerImplTest, messageListenerReturnsFailureTest) {
  auto consumer = createConsumer();
  auto listener = [](const Message&) { return ConsumeResult::FAILURE; };

  consumer->registerMessageListener(listener);

  auto& stored_listener = consumer->messageListener();
  ASSERT_TRUE(static_cast<bool>(stored_listener));

  auto msg = Message::newBuilder().withTopic("test-topic").build();
  EXPECT_EQ(ConsumeResult::FAILURE, stored_listener(*msg));
}

TEST(PushConsumerImplTest, messageListenerEmptyByDefaultTest) {
  auto consumer = createConsumer();
  auto& listener = consumer->messageListener();
  // Default-constructed std::function should be empty
  EXPECT_FALSE(static_cast<bool>(listener));
}

TEST(PushConsumerImplTest, getProcessQueueTableSizeEmptyTest) {
  auto consumer = createConsumer();
  EXPECT_EQ(0u, consumer->getProcessQueueTableSize());
}

TEST(PushConsumerImplTest, groupNameTest) {
  auto consumer = createConsumer("my-consumer-group");
  EXPECT_EQ("my-consumer-group", consumer->groupName());
}

TEST(PushConsumerImplTest, groupNameDefaultTest) {
  auto consumer = createConsumer();
  EXPECT_EQ("test-group", consumer->groupName());
}

TEST(PushConsumerImplTest, maxCachedMessageQuantityTest) {
  auto consumer = createConsumer();
  EXPECT_EQ(MixAll::DEFAULT_CACHED_MESSAGE_COUNT, consumer->maxCachedMessageQuantity());
}

TEST(PushConsumerImplTest, maxCachedMessageMemoryTest) {
  auto consumer = createConsumer();
  EXPECT_EQ(MixAll::DEFAULT_CACHED_MESSAGE_MEMORY, consumer->maxCachedMessageMemory());
}

// shutdown() must be idempotent and safe to call repeatedly, including the
// implicit call from the destructor. This underpins the public
// PushConsumer::shutdown() contract (explicit shutdown followed by destruction).
TEST(PushConsumerImplTest, shutdownIsIdempotentTest) {
  auto consumer = createConsumer();
  consumer->shutdown();
  consumer->shutdown();
  // Destruction here triggers shutdown() once more; must not crash.
}

ROCKETMQ_NAMESPACE_END
