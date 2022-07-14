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

#include <string>

#include "ExpressionType.h"
#include "Message.h"

ROCKETMQ_NAMESPACE_BEGIN

/**
 * Server supported message filtering expression. At present, two types are supported: tag and SQL92.
 */
struct FilterExpression {
  FilterExpression(std::string expression, ExpressionType expression_type = ExpressionType::TAG)
      : content_(std::move(expression)), type_(expression_type), version_(std::chrono::steady_clock::now()) {
    if (ExpressionType::TAG == type_ && content_.empty()) {
      content_ = WILD_CARD_TAG;
    }
  }

  bool accept(const Message& message) const;

  std::string content_;
  ExpressionType type_;
  std::chrono::steady_clock::time_point version_;

  static const char* WILD_CARD_TAG;
};

ROCKETMQ_NAMESPACE_END