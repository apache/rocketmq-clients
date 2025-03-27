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

#include <chrono>
#include <memory>

#include "UniqueIdGenerator.h"

ROCKETMQ_NAMESPACE_BEGIN

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
  message_->group_.swap(group);
  return *this;
}

MessageBuilder& MessageBuilder::withProperties(std::unordered_map<std::string, std::string> properties) {
  message_->properties_ = std::move(properties);
  return *this;
}

MessageBuilder& MessageBuilder::availableAfter(std::chrono::system_clock::time_point delivery_timepoint) {
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