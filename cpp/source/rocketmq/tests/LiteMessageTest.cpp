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

#include "Protocol.h"
#include "rocketmq/Message.h"
#include "rocketmq/OffsetOption.h"
#include "gtest/gtest.h"

ROCKETMQ_NAMESPACE_BEGIN

// ============================================================================
// Message Lite Topic Tests
// ============================================================================

TEST(LiteMessageTest, testLiteTopicMessageBuilder) {
  auto message = Message::newBuilder()
                     .withTopic("LiteTopic")
                     .withBody("Test lite message")
                     .withTag("LiteTag")
                     .withKeys({"key1"})
                     .withLiteTopic("lite-topic-1")
                     .build();

  ASSERT_NE(message, nullptr);
  EXPECT_EQ(message->topic(), "LiteTopic");
  EXPECT_EQ(message->body(), "Test lite message");
  EXPECT_EQ(message->tag(), "LiteTag");
  EXPECT_EQ(message->liteTopic(), "lite-topic-1");
}

TEST(LiteMessageTest, testDefaultLiteTopicIsEmpty) {
  auto message = Message::newBuilder()
                     .withTopic("NormalTopic")
                     .withBody("Normal message")
                     .build();

  ASSERT_NE(message, nullptr);
  EXPECT_TRUE(message->liteTopic().empty());
}

TEST(LiteMessageTest, testLiteTopicWithDifferentNames) {
  std::vector<std::string> lite_topic_names = {
      "lite-topic-1",
      "lite_topic_2",
      "lite.topic.3",
      "a",
      std::string(64, 'x'),
  };

  for (const auto& name : lite_topic_names) {
    auto message = Message::newBuilder()
                       .withTopic("ParentTopic")
                       .withBody("body")
                       .withLiteTopic(name)
                       .build();
    ASSERT_NE(message, nullptr);
    EXPECT_EQ(message->liteTopic(), name);
  }
}

TEST(LiteMessageTest, testLiteTopicSetterWithSpaces) {
  // Blank lite topic should throw, mirroring Java's testLiteTopicSetterWithSpaces
  EXPECT_THROW(
      Message::newBuilder().withLiteTopic("  "),
      std::invalid_argument);
}

TEST(LiteMessageTest, testLiteTopicSetterWithTab) {
  EXPECT_THROW(
      Message::newBuilder().withLiteTopic("\t"),
      std::invalid_argument);
}

TEST(LiteMessageTest, testLiteTopicSetterWithEmpty) {
  EXPECT_THROW(
      Message::newBuilder().withLiteTopic(""),
      std::invalid_argument);
}

TEST(LiteMessageTest, testLiteTopicSetter) {
  // Basic lite topic setter, mirroring Java's testLiteTopicSetter
  auto message = Message::newBuilder()
                     .withLiteTopic("liteTopicA")
                     .withTopic("TestTopic")
                     .withBody("Test body")
                     .build();
  EXPECT_FALSE(message->liteTopic().empty());
  EXPECT_EQ(message->liteTopic(), "liteTopicA");
}

TEST(LiteMessageTest, testBuildDefaults) {
  // Default message should have no liteTopic, no group, no deliveryTimestamp, no priority
  // Mirroring Java's testBuild
  auto message = Message::newBuilder()
                     .withTopic("TestTopic")
                     .withBody("Test body")
                     .build();
  EXPECT_TRUE(message->liteTopic().empty());
  EXPECT_TRUE(message->group().empty());
  EXPECT_EQ(message->deliveryTimestamp().time_since_epoch().count(), 0);
  EXPECT_EQ(message->priority(), -1);
}

TEST(LiteMessageTest, testMessageTypeConflict_DeliveryAndLite) {
  // deliveryTimestamp + liteTopic conflict
  auto now = std::chrono::system_clock::now();
  EXPECT_THROW(
      Message::newBuilder().availableAfter(now).withLiteTopic("HW"),
      std::invalid_argument);
}

TEST(LiteMessageTest, testMessageTypeConflict_DeliveryAndGroup) {
  auto now = std::chrono::system_clock::now();
  EXPECT_THROW(
      Message::newBuilder().availableAfter(now).withGroup("HW"),
      std::invalid_argument);
}

TEST(LiteMessageTest, testMessageTypeConflict_DeliveryAndPriority) {
  auto now = std::chrono::system_clock::now();
  EXPECT_THROW(
      Message::newBuilder().availableAfter(now).withPriority(1),
      std::invalid_argument);
}

TEST(LiteMessageTest, testMessageTypeConflict_GroupAndLite) {
  EXPECT_THROW(
      Message::newBuilder().withGroup("HW").withLiteTopic("HW"),
      std::invalid_argument);
}

TEST(LiteMessageTest, testMessageTypeConflict_GroupAndDelivery) {
  auto now = std::chrono::system_clock::now();
  EXPECT_THROW(
      Message::newBuilder().withGroup("HW").availableAfter(now),
      std::invalid_argument);
}

TEST(LiteMessageTest, testMessageTypeConflict_GroupAndPriority) {
  EXPECT_THROW(
      Message::newBuilder().withGroup("HW").withPriority(1),
      std::invalid_argument);
}

TEST(LiteMessageTest, testMessageTypeConflict_LiteAndGroup) {
  EXPECT_THROW(
      Message::newBuilder().withLiteTopic("HW").withGroup("HW"),
      std::invalid_argument);
}

TEST(LiteMessageTest, testMessageTypeConflict_LiteAndDelivery) {
  auto now = std::chrono::system_clock::now();
  EXPECT_THROW(
      Message::newBuilder().withLiteTopic("HW").availableAfter(now),
      std::invalid_argument);
}

TEST(LiteMessageTest, testMessageTypeConflict_LiteAndPriority) {
  EXPECT_THROW(
      Message::newBuilder().withLiteTopic("HW").withPriority(1),
      std::invalid_argument);
}

TEST(LiteMessageTest, testMessageTypeConflict_PriorityAndDelivery) {
  auto now = std::chrono::system_clock::now();
  EXPECT_THROW(
      Message::newBuilder().withPriority(1).availableAfter(now),
      std::invalid_argument);
}

TEST(LiteMessageTest, testMessageTypeConflict_PriorityAndLite) {
  EXPECT_THROW(
      Message::newBuilder().withPriority(1).withLiteTopic("HW"),
      std::invalid_argument);
}

TEST(LiteMessageTest, testMessageTypeConflict_PriorityAndGroup) {
  EXPECT_THROW(
      Message::newBuilder().withPriority(1).withGroup("HW"),
      std::invalid_argument);
}

TEST(LiteMessageTest, testLiteTopicWithProperties) {
  std::unordered_map<std::string, std::string> properties;
  properties["custom_key"] = "custom_value";

  auto message = Message::newBuilder()
                     .withTopic("LiteTopic")
                     .withBody("Lite message with properties")
                     .withLiteTopic("my-lite-topic")
                     .withProperties(properties)
                     .build();

  ASSERT_NE(message, nullptr);
  EXPECT_EQ(message->liteTopic(), "my-lite-topic");
  EXPECT_EQ(message->properties().size(), 1);
  EXPECT_EQ(message->properties().at("custom_key"), "custom_value");
}

// ============================================================================
// OffsetOption Tests
// ============================================================================

TEST(OffsetOptionTest, testLastOffset) {
  auto option = OffsetOption::lastOffset();
  EXPECT_EQ(option.type(), OffsetOption::Type::POLICY);
  EXPECT_EQ(option.value(), static_cast<int64_t>(OffsetOption::Policy::LAST));
}

TEST(OffsetOptionTest, testMinOffset) {
  auto option = OffsetOption::minOffset();
  EXPECT_EQ(option.type(), OffsetOption::Type::POLICY);
  EXPECT_EQ(option.value(), static_cast<int64_t>(OffsetOption::Policy::MIN));
}

TEST(OffsetOptionTest, testMaxOffset) {
  auto option = OffsetOption::maxOffset();
  EXPECT_EQ(option.type(), OffsetOption::Type::POLICY);
  EXPECT_EQ(option.value(), static_cast<int64_t>(OffsetOption::Policy::MAX));
}

TEST(OffsetOptionTest, testOfOffset) {
  auto option = OffsetOption::ofOffset(100);
  EXPECT_EQ(option.type(), OffsetOption::Type::OFFSET);
  EXPECT_EQ(option.value(), 100);
}

TEST(OffsetOptionTest, testOfTailN) {
  auto option = OffsetOption::ofTailN(50);
  EXPECT_EQ(option.type(), OffsetOption::Type::TAIL_N);
  EXPECT_EQ(option.value(), 50);
}

TEST(OffsetOptionTest, testOfTimestamp) {
  auto option = OffsetOption::ofTimestamp(1700000000000);
  EXPECT_EQ(option.type(), OffsetOption::Type::TIMESTAMP);
  EXPECT_EQ(option.value(), 1700000000000);
}

TEST(OffsetOptionTest, testEquality) {
  auto opt1 = OffsetOption::lastOffset();
  auto opt2 = OffsetOption::lastOffset();
  auto opt3 = OffsetOption::minOffset();

  EXPECT_EQ(opt1, opt2);
  EXPECT_NE(opt1, opt3);
}

TEST(OffsetOptionTest, testNegativeOffsetThrows) {
  EXPECT_THROW(OffsetOption::ofOffset(-1), std::invalid_argument);
  EXPECT_THROW(OffsetOption::ofTailN(-1), std::invalid_argument);
  EXPECT_THROW(OffsetOption::ofTimestamp(-1), std::invalid_argument);
}

TEST(OffsetOptionTest, testZeroOffsetIsValid) {
  EXPECT_NO_THROW(OffsetOption::ofOffset(0));
  EXPECT_NO_THROW(OffsetOption::ofTailN(0));
  EXPECT_NO_THROW(OffsetOption::ofTimestamp(0));
}

ROCKETMQ_NAMESPACE_END
