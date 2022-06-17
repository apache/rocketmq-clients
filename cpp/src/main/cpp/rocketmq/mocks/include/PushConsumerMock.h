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
#pragma once

#include <system_error>

#include "ConsumerMock.h"
#include "PushConsumer.h"

ROCKETMQ_NAMESPACE_BEGIN

class PushConsumerMock : virtual public PushConsumer, virtual public ConsumerMock {
public:
  MOCK_METHOD(MessageModel, messageModel, (), (const override));

  MOCK_METHOD(void, ack, (const MQMessageExt&, const std::function<void(const std::error_code&)>&), (override));

  MOCK_METHOD(void, forwardToDeadLetterQueue, (const MQMessageExt&, const std::function<void(bool)>&), (override));

  MOCK_METHOD(const Executor&, customExecutor, (), (const override));

  MOCK_METHOD(uint32_t, consumeBatchSize, (), (const override));

  MOCK_METHOD(int32_t, maxDeliveryAttempts, (), (const override));

  MOCK_METHOD(void, updateOffset, (const MQMessageQueue&, int64_t), (override));

  MOCK_METHOD(bool, receiveMessage, (const MQMessageQueue&, const FilterExpression&), (override));

  MOCK_METHOD(MessageListener*, messageListener, (), (override));

  MOCK_METHOD(void, setOffsetStore, (std::unique_ptr<OffsetStore>), (override));
};

ROCKETMQ_NAMESPACE_END