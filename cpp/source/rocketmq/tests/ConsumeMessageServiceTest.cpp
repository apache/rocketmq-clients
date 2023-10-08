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

#include "ConsumeMessageServiceImpl.h"
#include "PushConsumerImpl.h"
#include "gtest/gtest.h"
#include "rocketmq/ConsumeResult.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

TEST(ConsumeMessageServiceTest, testLifecycle) {
  auto listener = [](const Message&) { return ConsumeResult::SUCCESS; };
  std::weak_ptr<PushConsumerImpl> consumer;
  auto svc = std::make_shared<ConsumeMessageServiceImpl>(consumer, 2, listener);
  svc->start();
  ASSERT_EQ(State::STARTED, svc->state());

  svc->shutdown();
  ASSERT_EQ(State::STOPPED, svc->state());
}

ROCKETMQ_NAMESPACE_END