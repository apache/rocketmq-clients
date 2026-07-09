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
#include "rocketmq/Message.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <stdexcept>

#include "UniqueIdGenerator.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace {
bool isBlank(const std::string& s) {
  return std::all_of(s.begin(), s.end(), [](unsigned char c) { return std::isspace(c); });
}
} // namespace

Message::Message() {
  id_ = UniqueIdGenerator::instance().next();
}

MessageBuilder Message::newBuilder() {
  return {};
}

MessageBuilder::MessageBuilder() : message_(new Message()) {
}

MessageBuilder& MessageBuilder::withTopic(std::string topic) {
  message_->topic_.swap(topic);
  return *this;
}

MessageBuilder& MessageBuilder::withTag(std::string tag) {
  message_->tag_.swap(tag);
  return *this;
}

MessageBuilder& MessageBuilder::withKeys(std::vector<std::string> keys) {
  message_->keys_.swap(keys);
  return *this;
}

MessageBuilder& MessageBuilder::withTraceContext(std::string trace_context) {
  message_->trace_context_.swap(trace_context);
  return *this;
}

MessageBuilder& MessageBuilder::withBody(std::string body) {
  message_->body_.swap(body);
  return *this;
}

MessageBuilder& MessageBuilder::withGroup(std::string group) {
  if (message_->delivery_timestamp_.time_since_epoch().count()) {
    throw std::invalid_argument("messageGroup and deliveryTimestamp should not be set at same time");
  }
  if (!message_->lite_topic_.empty()) {
    throw std::invalid_argument("messageGroup and liteTopic should not be set at same time");
  }
  if (message_->priority_ >= 0) {
    throw std::invalid_argument("messageGroup and priority should not be set at same time");
  }
  if (group.empty() || isBlank(group)) {
    throw std::invalid_argument("messageGroup should not be blank");
  }
  message_->group_.swap(group);
  return *this;
}

MessageBuilder& MessageBuilder::withLiteTopic(std::string lite_topic) {
  if (message_->delivery_timestamp_.time_since_epoch().count()) {
    throw std::invalid_argument("liteTopic and deliveryTimestamp should not be set at same time");
  }
  if (!message_->group_.empty()) {
    throw std::invalid_argument("liteTopic and messageGroup should not be set at same time");
  }
  if (message_->priority_ >= 0) {
    throw std::invalid_argument("liteTopic and priority should not be set at same time");
  }
  if (lite_topic.empty() || isBlank(lite_topic)) {
    throw std::invalid_argument("liteTopic should not be blank");
  }
  message_->lite_topic_.swap(lite_topic);
  return *this;
}

MessageBuilder& MessageBuilder::withPriority(std::int32_t priority) {
  if (message_->delivery_timestamp_.time_since_epoch().count()) {
    throw std::invalid_argument("priority and deliveryTimestamp should not be set at same time");
  }
  if (!message_->group_.empty()) {
    throw std::invalid_argument("priority and messageGroup should not be set at same time");
  }
  if (!message_->lite_topic_.empty()) {
    throw std::invalid_argument("priority and liteTopic should not be set at same time");
  }
  message_->priority_ = priority;
  return *this;
}

MessageBuilder& MessageBuilder::withProperties(std::unordered_map<std::string, std::string> properties) {
  message_->properties_ = std::move(properties);
  return *this;
}

MessageBuilder& MessageBuilder::availableAfter(std::chrono::system_clock::time_point delivery_timepoint) {
  if (!message_->group_.empty()) {
    throw std::invalid_argument("deliveryTimestamp and messageGroup should not be set at same time");
  }
  if (!message_->lite_topic_.empty()) {
    throw std::invalid_argument("deliveryTimestamp and liteTopic should not be set at same time");
  }
  if (message_->priority_ >= 0) {
    throw std::invalid_argument("deliveryTimestamp and priority should not be set at same time");
  }
  message_->delivery_timestamp_ = delivery_timepoint;
  return *this;
}

MessageConstPtr MessageBuilder::build() {
  return std::move(message_);
}

MessageBuilder& MessageBuilder::withId(std::string id) {
  message_->id_ = std::move(id);
  return *this;
}

MessageBuilder& MessageBuilder::withBornTime(std::chrono::system_clock::time_point born_time) {
  message_->born_time_ = born_time;
  return *this;
}

MessageBuilder& MessageBuilder::withBornHost(std::string born_host) {
  message_->born_host_ = std::move(born_host);
  return *this;
}

ROCKETMQ_NAMESPACE_END