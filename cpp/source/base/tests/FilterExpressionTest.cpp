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
#include "gtest/gtest.h"
#include "rocketmq/FilterExpression.h"
#include "rocketmq/Message.h"

ROCKETMQ_NAMESPACE_BEGIN

class FilterExpressionTest : public testing::Test {
protected:
  static MessageConstPtr makeMessage(const std::string& topic, const std::string& tag) {
    return Message::newBuilder().withTopic(topic).withTag(tag).withBody("body").build();
  }
};

TEST_F(FilterExpressionTest, wildcardTagAcceptsAnyMessage) {
  FilterExpression expr("*", ExpressionType::TAG);
  EXPECT_TRUE(expr.accept(*makeMessage("topic", "tagA")));
  EXPECT_TRUE(expr.accept(*makeMessage("topic", "tagB")));
  EXPECT_TRUE(expr.accept(*makeMessage("topic", "")));
}

TEST_F(FilterExpressionTest, emptyTagBecomesWildcard) {
  FilterExpression expr("", ExpressionType::TAG);
  EXPECT_EQ("*", expr.content_);
  EXPECT_TRUE(expr.accept(*makeMessage("topic", "anyTag")));
}

TEST_F(FilterExpressionTest, exactTagMatch) {
  FilterExpression expr("tagA", ExpressionType::TAG);
  EXPECT_TRUE(expr.accept(*makeMessage("topic", "tagA")));
  EXPECT_FALSE(expr.accept(*makeMessage("topic", "tagB")));
  EXPECT_FALSE(expr.accept(*makeMessage("topic", "")));
}

TEST_F(FilterExpressionTest, sql92AlwaysAccepts) {
  // SQL92 filtering is done server-side; client always accepts
  FilterExpression expr("price > 100", ExpressionType::SQL92);
  EXPECT_TRUE(expr.accept(*makeMessage("topic", "anyTag")));
  EXPECT_TRUE(expr.accept(*makeMessage("topic", "")));
}

TEST_F(FilterExpressionTest, versionIsSetOnConstruction) {
  auto before = std::chrono::steady_clock::now();
  FilterExpression expr("tag", ExpressionType::TAG);
  auto after = std::chrono::steady_clock::now();
  EXPECT_GE(expr.version_, before);
  EXPECT_LE(expr.version_, after);
}

ROCKETMQ_NAMESPACE_END
