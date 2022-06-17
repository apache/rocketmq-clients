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

#include "absl/strings/string_view.h"

#include "CommandCustomHeader.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class QueryRouteRequestHeader : public CommandCustomHeader {
public:
  ~QueryRouteRequestHeader() override = default;

  void topic(absl::string_view topic) {
    topic_ = std::string(topic.data(), topic.length());
  }

  const std::string& topic() const {
    return topic_;
  }

  void encode(google::protobuf::Value& root) const override;

private:
  std::string topic_;
};

ROCKETMQ_NAMESPACE_END