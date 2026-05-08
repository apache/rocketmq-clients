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

#include "rocketmq/Message.h"
#include "gtest/gtest.h"

ROCKETMQ_NAMESPACE_BEGIN

TEST(PriorityMessageTest, testPriorityMessageBuilder) {
  // Test creating a priority message with builder
  auto message = Message::newBuilder()
                     .withTopic("PriorityTopic")
                     .withBody("Test priority message")
                     .withTag("PriorityTag")
                     .withKeys({"key1", "key2"})
                     .withPriority(5)
                     .build();

  ASSERT_NE(message, nullptr);
  EXPECT_EQ(message->topic(), "PriorityTopic");
  EXPECT_EQ(message->body(), "Test priority message");
  EXPECT_EQ(message->tag(), "PriorityTag");
  EXPECT_EQ(message->priority(), 5);
  
  auto& keys = message->keys();
  ASSERT_EQ(keys.size(), 2);
  EXPECT_EQ(keys[0], "key1");
  EXPECT_EQ(keys[1], "key2");
}

TEST(PriorityMessageTest, testDefaultPriority) {
  // Test that default priority is -1 (not set)
  auto message = Message::newBuilder()
                     .withTopic("NormalTopic")
                     .withBody("Normal message")
                     .build();

  ASSERT_NE(message, nullptr);
  EXPECT_EQ(message->priority(), -1);
}

TEST(PriorityMessageTest, testDifferentPriorityLevels) {
  // Test different priority levels
  for (int i = 0; i <= 10; ++i) {
    auto message = Message::newBuilder()
                       .withTopic("PriorityTopic")
                       .withBody("Message with priority " + std::to_string(i))
                       .withPriority(i)
                       .build();

    ASSERT_NE(message, nullptr);
    EXPECT_EQ(message->priority(), i);
  }
}

TEST(PriorityMessageTest, testPriorityMessageWithProperties) {
  // Test priority message with custom properties
  std::unordered_map<std::string, std::string> properties;
  properties["custom_key"] = "custom_value";
  properties["another_key"] = "another_value";

  auto message = Message::newBuilder()
                     .withTopic("PriorityTopic")
                     .withBody("Priority message with properties")
                     .withPriority(8)
                     .withProperties(properties)
                     .build();

  ASSERT_NE(message, nullptr);
  EXPECT_EQ(message->priority(), 8);
  
  auto& msg_properties = message->properties();
  EXPECT_EQ(msg_properties.size(), 2);
  EXPECT_EQ(msg_properties.at("custom_key"), "custom_value");
  EXPECT_EQ(msg_properties.at("another_key"), "another_value");
}

ROCKETMQ_NAMESPACE_END
