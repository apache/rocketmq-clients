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
#include <vector>

#include "ClientManagerMock.h"
#include "MixAll.h"
#include "ProcessQueueImpl.h"
#include "PushConsumerImpl.h"
#include "gtest/gtest.h"
#include "gmock/gmock.h"
#include "rocketmq/FilterExpression.h"
#include "rocketmq/Message.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace {

rmq::MessageQueue createMessageQueue(const std::string& topic) {
  rmq::MessageQueue mq;
  mq.mutable_topic()->set_name(topic);
  mq.set_id(0);
  mq.mutable_broker()->set_name("broker-0");
  mq.mutable_broker()->set_id(MixAll::MASTER_BROKER_ID);
  mq.mutable_broker()->mutable_endpoints()->set_scheme(rmq::AddressScheme::IPv4);
  auto* addr = mq.mutable_broker()->mutable_endpoints()->add_addresses();
  addr->set_host("10.0.0.1");
  addr->set_port(8080);
  mq.set_permission(rmq::Permission::READ_WRITE);
  return mq;
}

MessageConstSharedPtr makeMessage(const std::string& topic, const std::string& body) {
  auto ptr = Message::newBuilder().withTopic(topic).withBody(body).build();
  return MessageConstSharedPtr(std::move(ptr));
}

}  // namespace

class ProcessQueueImplTest : public testing::Test {
protected:
  void SetUp() override {
    consumer_ = std::make_shared<PushConsumerImpl>("test-group");
    message_queue_ = createMessageQueue("test-topic");
    filter_expression_ = FilterExpression("test-tag", TAG);
    client_manager_ = std::make_shared<testing::NiceMock<ClientManagerMock>>();
    process_queue_.reset(
        new ProcessQueueImpl(message_queue_, filter_expression_, consumer_, client_manager_));
  }

  void TearDown() override {
    process_queue_.reset();
    consumer_.reset();
  }

  std::shared_ptr<PushConsumerImpl> consumer_;
  rmq::MessageQueue message_queue_;
  FilterExpression filter_expression_{"*", TAG};
  std::shared_ptr<testing::NiceMock<ClientManagerMock>> client_manager_;
  std::unique_ptr<ProcessQueueImpl> process_queue_;
};

TEST_F(ProcessQueueImplTest, cachedMessageQuantityStartsAtZeroTest) {
  EXPECT_EQ(0u, process_queue_->cachedMessageQuantity());
}

TEST_F(ProcessQueueImplTest, cachedMessageMemoryStartsAtZeroTest) {
  EXPECT_EQ(0u, process_queue_->cachedMessageMemory());
}

TEST_F(ProcessQueueImplTest, topicReturnsCorrectValueTest) {
  EXPECT_EQ("test-topic", process_queue_->topic());
}

TEST_F(ProcessQueueImplTest, getFilterExpressionReturnsConfiguredFilterTest) {
  const auto& fe = process_queue_->getFilterExpression();
  EXPECT_EQ("test-tag", fe.content_);
  EXPECT_EQ(TAG, fe.type_);
}

TEST_F(ProcessQueueImplTest, getFilterExpressionSqlTypeTest) {
  FilterExpression sql_filter("price > 10", SQL92);
  ProcessQueueImpl pq(message_queue_, sql_filter, consumer_, client_manager_);
  const auto& fe = pq.getFilterExpression();
  EXPECT_EQ("price > 10", fe.content_);
  EXPECT_EQ(SQL92, fe.type_);
}

TEST_F(ProcessQueueImplTest, accountCacheUpdatesQuantityAndMemoryTest) {
  std::vector<MessageConstSharedPtr> messages;
  messages.push_back(makeMessage("test-topic", "body12345"));  // 9 bytes
  messages.push_back(makeMessage("test-topic", "abcde"));      // 5 bytes

  process_queue_->accountCache(messages);

  EXPECT_EQ(2u, process_queue_->cachedMessageQuantity());
  EXPECT_EQ(14u, process_queue_->cachedMessageMemory());
}

TEST_F(ProcessQueueImplTest, accountCacheMultipleBatchesAccumulateTest) {
  std::vector<MessageConstSharedPtr> batch1;
  batch1.push_back(makeMessage("test-topic", "aaa"));  // 3 bytes
  process_queue_->accountCache(batch1);

  std::vector<MessageConstSharedPtr> batch2;
  batch2.push_back(makeMessage("test-topic", "bbbbb"));  // 5 bytes
  process_queue_->accountCache(batch2);

  EXPECT_EQ(2u, process_queue_->cachedMessageQuantity());
  EXPECT_EQ(8u, process_queue_->cachedMessageMemory());
}

TEST_F(ProcessQueueImplTest, releaseDecrementsCacheTest) {
  std::vector<MessageConstSharedPtr> messages;
  messages.push_back(makeMessage("test-topic", "body12345"));  // 9 bytes
  messages.push_back(makeMessage("test-topic", "abcde"));      // 5 bytes
  process_queue_->accountCache(messages);

  // Release one message with body size 9
  process_queue_->release(9);

  EXPECT_EQ(1u, process_queue_->cachedMessageQuantity());
  EXPECT_EQ(5u, process_queue_->cachedMessageMemory());
}

TEST_F(ProcessQueueImplTest, releaseAllMessagesTest) {
  std::vector<MessageConstSharedPtr> messages;
  messages.push_back(makeMessage("test-topic", "abc"));  // 3 bytes
  process_queue_->accountCache(messages);

  process_queue_->release(3);

  EXPECT_EQ(0u, process_queue_->cachedMessageQuantity());
  EXPECT_EQ(0u, process_queue_->cachedMessageMemory());
}

TEST_F(ProcessQueueImplTest, shouldThrottleReturnsFalseWhenCacheEmptyTest) {
  EXPECT_FALSE(process_queue_->shouldThrottle());
}

TEST_F(ProcessQueueImplTest, shouldThrottleReturnsFalseBelowThresholdTest) {
  // Cache a few messages, well below DEFAULT_CACHED_MESSAGE_COUNT (1024)
  std::vector<MessageConstSharedPtr> messages;
  for (int i = 0; i < 10; i++) {
    messages.push_back(makeMessage("test-topic", "x"));
  }
  process_queue_->accountCache(messages);

  EXPECT_FALSE(process_queue_->shouldThrottle());
}

TEST_F(ProcessQueueImplTest, shouldThrottleReturnsTrueWhenQuantityExceedsThresholdTest) {
  // DEFAULT_CACHED_MESSAGE_COUNT is 1024
  uint32_t threshold = MixAll::DEFAULT_CACHED_MESSAGE_COUNT;
  std::vector<MessageConstSharedPtr> messages;
  for (uint32_t i = 0; i <= threshold; i++) {
    messages.push_back(makeMessage("test-topic", "x"));
  }
  process_queue_->accountCache(messages);

  EXPECT_TRUE(process_queue_->shouldThrottle());
}

TEST_F(ProcessQueueImplTest, shouldThrottleReturnsFalseAfterReleasingMessagesTest) {
  // Exceed threshold
  uint32_t threshold = MixAll::DEFAULT_CACHED_MESSAGE_COUNT;
  std::vector<MessageConstSharedPtr> messages;
  for (uint32_t i = 0; i <= threshold; i++) {
    messages.push_back(makeMessage("test-topic", "x"));
  }
  process_queue_->accountCache(messages);
  EXPECT_TRUE(process_queue_->shouldThrottle());

  // Release enough messages to drop below threshold (need to go below 1024)
  process_queue_->release(1);
  process_queue_->release(1);
  EXPECT_FALSE(process_queue_->shouldThrottle());
}

TEST_F(ProcessQueueImplTest, shouldThrottleReturnsFalseWithNullConsumerTest) {
  // Create ProcessQueue with an expired (null) consumer weak_ptr
  std::weak_ptr<PushConsumerImpl> null_consumer;
  ProcessQueueImpl pq(message_queue_, filter_expression_, null_consumer, client_manager_);

  // shouldThrottle returns false when consumer is gone
  EXPECT_FALSE(pq.shouldThrottle());
}

TEST_F(ProcessQueueImplTest, accountCacheWithNullConsumerDoesNotIncrementTest) {
  std::weak_ptr<PushConsumerImpl> null_consumer;
  ProcessQueueImpl pq(message_queue_, filter_expression_, null_consumer, client_manager_);

  std::vector<MessageConstSharedPtr> messages;
  messages.push_back(makeMessage("test-topic", "body"));
  pq.accountCache(messages);

  // accountCache returns early when consumer is null, so cache stays at 0
  EXPECT_EQ(0u, pq.cachedMessageQuantity());
  EXPECT_EQ(0u, pq.cachedMessageMemory());
}

TEST_F(ProcessQueueImplTest, expiredReturnsFalseImmediatelyAfterCreationTest) {
  // Just created, idle_since_ = now, so duration < 120s threshold
  EXPECT_FALSE(process_queue_->expired());
}

TEST_F(ProcessQueueImplTest, expiredReturnsFalseAfterSyncIdleStateTest) {
  // Sync idle state resets the timer
  process_queue_->syncIdleState();
  EXPECT_FALSE(process_queue_->expired());
}

TEST_F(ProcessQueueImplTest, simpleNameIsNotEmptyTest) {
  EXPECT_FALSE(process_queue_->simpleName().empty());
}

TEST_F(ProcessQueueImplTest, messageQueueReturnsCorrectQueueTest) {
  const auto& mq = process_queue_->messageQueue();
  EXPECT_EQ("test-topic", mq.topic().name());
  EXPECT_EQ(0, mq.id());
  EXPECT_EQ("broker-0", mq.broker().name());
}

TEST_F(ProcessQueueImplTest, getConsumerReturnsValidWeakPtrTest) {
  auto consumer = process_queue_->getConsumer().lock();
  EXPECT_NE(nullptr, consumer);
}

TEST_F(ProcessQueueImplTest, getClientManagerReturnsCorrectInstanceTest) {
  auto cm = process_queue_->getClientManager();
  EXPECT_EQ(client_manager_.get(), cm.get());
}

ROCKETMQ_NAMESPACE_END
