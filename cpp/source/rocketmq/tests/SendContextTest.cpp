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

#include "SendContext.h"
#include "gtest/gtest.h"

ROCKETMQ_NAMESPACE_BEGIN

TEST(SendContextTest, testBasics) {
  auto message = Message::newBuilder().withBody("body").withTopic("topic").withGroup("group0").withTag("TagA").build();
  auto callback = [](const std::error_code&, const SendReceipt&) {};
  std::weak_ptr<ProducerImpl> producer;
  std::vector<rmq::MessageQueue> message_queues;
  auto send_context = std::make_shared<SendContext>(producer, std::move(message), callback, message_queues);
  send_context.reset();
}

ROCKETMQ_NAMESPACE_END