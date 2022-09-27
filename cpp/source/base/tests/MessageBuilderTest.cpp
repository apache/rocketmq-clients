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
#include <vector>

#include "MessageExt.h"
#include "gtest/gtest.h"
#include "rocketmq/Message.h"

ROCKETMQ_NAMESPACE_BEGIN

class MessageBuilderTest : public testing::Test {
protected:
  std::string topic_{"TestTopic"};
  std::string tag_{"TagA"};
  std::vector<std::string> keys_{"k1", "k2"};
  std::string body_{"sample body content"};
  std::string group_{"message-group"};
};

TEST_F(MessageBuilderTest, testBuilder) {
  MessageConstPtr message =
      Message::newBuilder().withTopic(topic_).withTag(tag_).withKeys(keys_).withBody(body_).build();
  ASSERT_EQ(topic_, message->topic());
  ASSERT_EQ(tag_, message->tag());
  ASSERT_TRUE(keys_ == message->keys());
  ASSERT_EQ(body_, message->body());
  ASSERT_EQ(true, message->traceContext().empty());
}

TEST_F(MessageBuilderTest, testBuilder2) {
  for (std::size_t i = 0; i < 128; i++) {
    MessageConstPtr message =
        Message::newBuilder().withTopic(topic_).withTag(tag_).withKeys(keys_).withBody(body_).build();
    ASSERT_EQ(topic_, message->topic());
    ASSERT_EQ(tag_, message->tag());
    ASSERT_TRUE(keys_ == message->keys());
    ASSERT_EQ(body_, message->body());
    ASSERT_EQ(true, message->traceContext().empty());
  }
}

ROCKETMQ_NAMESPACE_END