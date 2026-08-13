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
#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <system_error>
#include <vector>

#include "ConsumeTask.h"
#include "ConsumeMessageService.h"
#include "ProcessQueue.h"
#include "PushConsumerImpl.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "rocketmq/ConsumeResult.h"
#include "rocketmq/Message.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace {

/**
 * Local mock of ConsumeMessageService matching the current interface.
 * The mock shipped in mocks/include/ is stale (missing submit, ack, nack,
 * forward, schedule, listener, consumer, etc.), so we define one here.
 */
class ConsumeTaskServiceMock : public ConsumeMessageService {
public:
  MOCK_METHOD(void, start, (), (override));
  MOCK_METHOD(void, shutdown, (), (override));
  MOCK_METHOD(void, dispatch, (std::shared_ptr<ProcessQueue>, std::vector<MessageConstSharedPtr>), (override));
  MOCK_METHOD(void, submit, (std::shared_ptr<ConsumeTask>), (override));
  MOCK_METHOD(MessageListener&, listener, (), (override));
  MOCK_METHOD(bool, preHandle, (const Message&), (override));
  MOCK_METHOD(bool, postHandle, (const Message&, ConsumeResult), (override));
  MOCK_METHOD(void, ack, (const Message&, std::function<void(const std::error_code&)>), (override));
  MOCK_METHOD(void, nack, (const Message&, std::function<void(const std::error_code&)>), (override));
  MOCK_METHOD(void, forward, (const Message&, std::function<void(const std::error_code&)>), (override));
  MOCK_METHOD(void, schedule, (std::shared_ptr<ConsumeTask>, std::chrono::milliseconds), (override));
  MOCK_METHOD(std::size_t, maxDeliveryAttempt, (), (override));
  MOCK_METHOD(std::weak_ptr<PushConsumerImpl>, consumer, (), (override));
};

/**
 * Local mock of ProcessQueue matching the current interface.
 * The mock shipped in mocks/include/ is stale as well.
 */
class ConsumeTaskProcessQueueMock : public ProcessQueue {
public:
  MOCK_METHOD(bool, expired, (), (const, override));
  MOCK_METHOD(void, callback, (std::shared_ptr<AsyncReceiveMessageCallback>), (override));
  MOCK_METHOD(void, receiveMessage, (std::string&), (override));
  MOCK_METHOD(std::string, topic, (), (const, override));
  MOCK_METHOD(std::weak_ptr<PushConsumerImpl>, getConsumer, (), (override));
  MOCK_METHOD(const std::string&, simpleName, (), (const, override));
  MOCK_METHOD(void, release, (uint64_t), (override));
  MOCK_METHOD(void, accountCache, (const std::vector<MessageConstSharedPtr>&), (override));
  MOCK_METHOD(std::uint64_t, cachedMessageQuantity, (), (const, override));
  MOCK_METHOD(std::uint64_t, cachedMessageMemory, (), (const, override));
  MOCK_METHOD(bool, shouldThrottle, (), (const, override));
  MOCK_METHOD(std::shared_ptr<ClientManager>, getClientManager, (), (override));
  MOCK_METHOD(void, syncIdleState, (), (override));
  MOCK_METHOD(const FilterExpression&, getFilterExpression, (), (const, override));
  MOCK_METHOD(const rmq::MessageQueue&, messageQueue, (), (const, override));
};

class ConsumeTaskTest : public testing::Test {
protected:
  static MessageConstSharedPtr buildMessage(const std::string& topic, const std::string& body) {
    return MessageConstSharedPtr(Message::newBuilder().withTopic(topic).withBody(body).build().release());
  }

  std::shared_ptr<testing::NiceMock<ConsumeTaskServiceMock>> service_{
      std::make_shared<testing::NiceMock<ConsumeTaskServiceMock>>()};
  std::shared_ptr<testing::NiceMock<ConsumeTaskProcessQueueMock>> pq_{
      std::make_shared<testing::NiceMock<ConsumeTaskProcessQueueMock>>()};
  std::weak_ptr<ProcessQueue> weak_pq_{pq_};
  ConsumeMessageServiceWeakPtr weak_service_{service_};
};

// --- Construction ---

TEST_F(ConsumeTaskTest, constructWithSingleMessageTest) {
  auto msg = buildMessage("topic", "body");
  ConsumeTask task(weak_service_, weak_pq_, msg);
  // Construction must not crash; fifo_ should remain false for a single message.
}

TEST_F(ConsumeTaskTest, constructWithBatchMessagesTest) {
  std::vector<MessageConstSharedPtr> msgs;
  msgs.push_back(buildMessage("topic", "body1"));
  msgs.push_back(buildMessage("topic", "body2"));
  ConsumeTask task(weak_service_, weak_pq_, std::move(msgs));
  // Construction must not crash; fifo_ should be set to true (>1 messages).
}

// --- submit() ---

TEST_F(ConsumeTaskTest, submitWithValidServiceTest) {
  auto msg = buildMessage("topic", "body");
  auto task = std::make_shared<ConsumeTask>(weak_service_, weak_pq_, msg);

  EXPECT_CALL(*service_, submit(testing::_)).Times(1);
  task->submit();
}

TEST_F(ConsumeTaskTest, submitWithExpiredServiceTest) {
  ConsumeMessageServiceWeakPtr expired;
  auto msg = buildMessage("topic", "body");
  auto task = std::make_shared<ConsumeTask>(expired, weak_pq_, msg);
  // Service is expired — submit() should return silently.
  task->submit();
}

// --- schedule() ---

TEST_F(ConsumeTaskTest, scheduleWithValidServiceTest) {
  auto msg = buildMessage("topic", "body");
  auto task = std::make_shared<ConsumeTask>(weak_service_, weak_pq_, msg);

  EXPECT_CALL(*service_, schedule(testing::_, std::chrono::milliseconds(1000))).Times(1);
  task->schedule();
}

TEST_F(ConsumeTaskTest, scheduleWithExpiredServiceTest) {
  ConsumeMessageServiceWeakPtr expired;
  auto msg = buildMessage("topic", "body");
  auto task = std::make_shared<ConsumeTask>(expired, weak_pq_, msg);
  // Service is expired — schedule() should return silently.
  task->schedule();
}

// --- process() early-return paths ---

TEST_F(ConsumeTaskTest, processWithExpiredServiceTest) {
  ConsumeMessageServiceWeakPtr expired;
  auto msg = buildMessage("topic", "body");
  auto task = std::make_shared<ConsumeTask>(expired, weak_pq_, msg);
  // Service is expired — process() returns early without touching any mock.
  task->process();
}

TEST_F(ConsumeTaskTest, processWithEmptyMessagesTest) {
  std::vector<MessageConstSharedPtr> empty;
  auto task = std::make_shared<ConsumeTask>(weak_service_, weak_pq_, std::move(empty));
  // No messages cached — process() should return after the emptiness check.
  task->process();
}

// Regression: the service is still alive but the owning PushConsumerImpl has
// already been destructed, so consumer().lock() yields null. process() must not
// dereference the null consumer (metrics/stats access), it must bail out before
// invoking the listener.
TEST_F(ConsumeTaskTest, processWithExpiredConsumerTest) {
  auto msg = buildMessage("topic", "body");
  auto task = std::make_shared<ConsumeTask>(weak_service_, weak_pq_, msg);

  bool listener_invoked = false;
  MessageListener listener = [&listener_invoked](const Message&) {
    listener_invoked = true;
    return ConsumeResult::SUCCESS;
  };

  // Consumer weak_ptr is expired (owning PushConsumerImpl already gone).
  EXPECT_CALL(*service_, consumer()).WillRepeatedly(testing::Return(std::weak_ptr<PushConsumerImpl>()));
  ON_CALL(*service_, listener()).WillByDefault(testing::ReturnRef(listener));

  // With no consumer, process() must not run the listener or ack/nack.
  EXPECT_CALL(*service_, preHandle(testing::_)).Times(0);
  EXPECT_CALL(*service_, ack(testing::_, testing::_)).Times(0);
  EXPECT_CALL(*service_, nack(testing::_, testing::_)).Times(0);

  task->process();  // must not dereference a null consumer
  EXPECT_FALSE(listener_invoked);
}

// --- process() state-machine: Consume → Ack on SUCCESS ---

TEST_F(ConsumeTaskTest, processConsumeSuccessCallsAckTest) {
  auto msg = buildMessage("topic", "body");
  auto task = std::make_shared<ConsumeTask>(weak_service_, weak_pq_, msg);

  auto consumer = std::make_shared<PushConsumerImpl>("test-group");
  MessageListener listener = [](const Message&) { return ConsumeResult::SUCCESS; };

  EXPECT_CALL(*service_, consumer()).WillRepeatedly(testing::Return(std::weak_ptr<PushConsumerImpl>(consumer)));
  EXPECT_CALL(*service_, listener()).WillRepeatedly(testing::ReturnRef(listener));
  EXPECT_CALL(*service_, preHandle(testing::_)).WillOnce(testing::Return(true));
  EXPECT_CALL(*service_, postHandle(testing::_, ConsumeResult::SUCCESS)).WillOnce(testing::Return(true));

  // On success process() calls ack(); the callback invokes onAck() which pops
  // the head message and resubmits the task for the remaining messages.
  EXPECT_CALL(*service_, ack(testing::_, testing::_))
      .WillOnce(testing::Invoke([](const Message&, std::function<void(const std::error_code&)> cb) {
        cb(std::error_code{});
      }));
  EXPECT_CALL(*pq_, release(testing::_)).Times(1);
  EXPECT_CALL(*service_, submit(testing::_)).Times(1);

  task->process();
}

// --- process() state-machine: Consume → Nack on FAILURE (non-FIFO) ---

TEST_F(ConsumeTaskTest, processConsumeFailureNonFifoCallsNackTest) {
  // Single message ⇒ fifo_ == false
  auto msg = buildMessage("topic", "body");
  auto task = std::make_shared<ConsumeTask>(weak_service_, weak_pq_, msg);

  auto consumer = std::make_shared<PushConsumerImpl>("test-group");
  MessageListener listener = [](const Message&) { return ConsumeResult::FAILURE; };

  EXPECT_CALL(*service_, consumer()).WillRepeatedly(testing::Return(std::weak_ptr<PushConsumerImpl>(consumer)));
  EXPECT_CALL(*service_, listener()).WillRepeatedly(testing::ReturnRef(listener));
  EXPECT_CALL(*service_, preHandle(testing::_)).WillOnce(testing::Return(true));
  EXPECT_CALL(*service_, postHandle(testing::_, ConsumeResult::FAILURE)).WillOnce(testing::Return(true));

  // On failure in non-FIFO mode process() calls nack(); the callback invokes
  // onNack() which pops the head message and resubmits the task.
  EXPECT_CALL(*service_, nack(testing::_, testing::_))
      .WillOnce(testing::Invoke([](const Message&, std::function<void(const std::error_code&)> cb) {
        cb(std::error_code{});
      }));
  EXPECT_CALL(*pq_, release(testing::_)).Times(1);
  EXPECT_CALL(*service_, submit(testing::_)).Times(1);

  task->process();
}

// --- process() state-machine: Consume → schedule retry on FAILURE (FIFO) ---

TEST_F(ConsumeTaskTest, processConsumeFailureFifoSchedulesRetryTest) {
  // Multiple messages ⇒ fifo_ == true
  std::vector<MessageConstSharedPtr> msgs;
  msgs.push_back(buildMessage("topic", "body1"));
  msgs.push_back(buildMessage("topic", "body2"));
  auto task = std::make_shared<ConsumeTask>(weak_service_, weak_pq_, std::move(msgs));

  auto consumer = std::make_shared<PushConsumerImpl>("test-group");
  MessageListener listener = [](const Message&) { return ConsumeResult::FAILURE; };

  EXPECT_CALL(*service_, consumer()).WillRepeatedly(testing::Return(std::weak_ptr<PushConsumerImpl>(consumer)));
  EXPECT_CALL(*service_, listener()).WillRepeatedly(testing::ReturnRef(listener));
  EXPECT_CALL(*service_, preHandle(testing::_)).WillOnce(testing::Return(true));
  EXPECT_CALL(*service_, postHandle(testing::_, ConsumeResult::FAILURE)).WillOnce(testing::Return(true));

  // In FIFO mode, failure does not call nack(); instead it increments
  // delivery_attempt and schedules a retry after 1 second.
  EXPECT_CALL(*service_, nack(testing::_, testing::_)).Times(0);
  EXPECT_CALL(*service_, schedule(testing::_, std::chrono::milliseconds(1000))).Times(1);

  task->process();
}

// --- onAck retry path: ack failure re-schedules ---

TEST_F(ConsumeTaskTest, processAckFailureSchedulesRetryTest) {
  auto msg = buildMessage("topic", "body");
  auto task = std::make_shared<ConsumeTask>(weak_service_, weak_pq_, msg);

  auto consumer = std::make_shared<PushConsumerImpl>("test-group");
  MessageListener listener = [](const Message&) { return ConsumeResult::SUCCESS; };

  EXPECT_CALL(*service_, consumer()).WillRepeatedly(testing::Return(std::weak_ptr<PushConsumerImpl>(consumer)));
  EXPECT_CALL(*service_, listener()).WillRepeatedly(testing::ReturnRef(listener));
  EXPECT_CALL(*service_, preHandle(testing::_)).WillOnce(testing::Return(true));
  EXPECT_CALL(*service_, postHandle(testing::_, ConsumeResult::SUCCESS)).WillOnce(testing::Return(true));

  // Simulate an ack RPC failure — onAck should set next_step_ back to Ack
  // and schedule a retry rather than popping the message.
  EXPECT_CALL(*service_, ack(testing::_, testing::_))
      .WillOnce(testing::Invoke([](const Message&, std::function<void(const std::error_code&)> cb) {
        cb(std::make_error_code(std::errc::connection_refused));
      }));
  EXPECT_CALL(*pq_, release(testing::_)).Times(0);
  EXPECT_CALL(*service_, schedule(testing::_, std::chrono::milliseconds(1000))).Times(1);

  task->process();
}

// --- onNack retry path: nack failure re-schedules ---

TEST_F(ConsumeTaskTest, processNackFailureSchedulesRetryTest) {
  auto msg = buildMessage("topic", "body");
  auto task = std::make_shared<ConsumeTask>(weak_service_, weak_pq_, msg);

  auto consumer = std::make_shared<PushConsumerImpl>("test-group");
  MessageListener listener = [](const Message&) { return ConsumeResult::FAILURE; };

  EXPECT_CALL(*service_, consumer()).WillRepeatedly(testing::Return(std::weak_ptr<PushConsumerImpl>(consumer)));
  EXPECT_CALL(*service_, listener()).WillRepeatedly(testing::ReturnRef(listener));
  EXPECT_CALL(*service_, preHandle(testing::_)).WillOnce(testing::Return(true));
  EXPECT_CALL(*service_, postHandle(testing::_, ConsumeResult::FAILURE)).WillOnce(testing::Return(true));

  // Simulate a nack RPC failure — onNack should set next_step_ back to Nack
  // and schedule a retry rather than popping the message.
  EXPECT_CALL(*service_, nack(testing::_, testing::_))
      .WillOnce(testing::Invoke([](const Message&, std::function<void(const std::error_code&)> cb) {
        cb(std::make_error_code(std::errc::connection_refused));
      }));
  EXPECT_CALL(*pq_, release(testing::_)).Times(0);
  EXPECT_CALL(*service_, schedule(testing::_, std::chrono::milliseconds(1000))).Times(1);

  task->process();
}

// --- pop() with expired ProcessQueue: must not crash ---

TEST_F(ConsumeTaskTest, processAckSuccessWithExpiredProcessQueueTest) {
  std::weak_ptr<ProcessQueue> expired_pq;
  auto msg = buildMessage("topic", "body");
  auto task = std::make_shared<ConsumeTask>(weak_service_, expired_pq, msg);

  auto consumer = std::make_shared<PushConsumerImpl>("test-group");
  MessageListener listener = [](const Message&) { return ConsumeResult::SUCCESS; };

  EXPECT_CALL(*service_, consumer()).WillRepeatedly(testing::Return(std::weak_ptr<PushConsumerImpl>(consumer)));
  EXPECT_CALL(*service_, listener()).WillRepeatedly(testing::ReturnRef(listener));
  EXPECT_CALL(*service_, preHandle(testing::_)).WillOnce(testing::Return(true));
  EXPECT_CALL(*service_, postHandle(testing::_, ConsumeResult::SUCCESS)).WillOnce(testing::Return(true));

  // ack succeeds; onAck calls pop() which finds the ProcessQueue expired —
  // it should return gracefully without crashing.
  EXPECT_CALL(*service_, ack(testing::_, testing::_))
      .WillOnce(testing::Invoke([](const Message&, std::function<void(const std::error_code&)> cb) {
        cb(std::error_code{});
      }));
  EXPECT_CALL(*service_, submit(testing::_)).Times(1);

  task->process();
}

} // namespace

ROCKETMQ_NAMESPACE_END
