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

#include <cassert>
#include <chrono>
#include <map>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

#include "RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class MessageBuilder;

/**
 * @brief Extension is intended for internal use only, which is subject to change without notice.
 */
struct Extension {
  std::chrono::system_clock::time_point store_time{std::chrono::system_clock::now()};
  std::string store_host;
  std::chrono::system_clock::time_point delivery_timepoint{std::chrono::system_clock::now()};
  std::uint16_t delivery_attempt{0};
  std::chrono::system_clock::time_point decode_time{std::chrono::system_clock::now()};
  std::chrono::duration<long long, std::nano> invisible_period{0};
  std::string receipt_handle;
  std::string target_endpoint;
  std::int32_t queue_id{0};
  std::int64_t offset{0};
  std::string nonce;
  std::string transaction_id;
  bool transactional{false};
};

class Message {
public:
  const std::string& id() const {
    return id_;
  }

  const std::string& topic() const {
    return topic_;
  }

  const std::string& tag() const {
    return tag_;
  }

  const std::vector<std::string>& keys() const {
    return keys_;
  }

  const std::string& traceContext() const {
    return trace_context_;
  }

  void traceContext(std::string&& trace_context) {
    trace_context_ = std::move(trace_context);
  }

  const std::string& bornHost() const {
    return born_host_;
  }

  const std::chrono::system_clock::time_point& bornTime() const {
    return born_time_;
  }

  std::chrono::system_clock::time_point deliveryTimestamp() const {
    return delivery_timestamp_;
  }

  const std::string& body() const {
    return body_;
  }

  const std::unordered_map<std::string, std::string>& properties() const {
    return properties_;
  }

  const std::string& group() const {
    return group_;
  }

  const Extension& extension() const {
    return extension_;
  }

  Extension& mutableExtension() {
    return extension_;
  }

  static MessageBuilder newBuilder();

protected:
  friend class MessageBuilder;

  Message();

private:
  std::string id_;
  std::string topic_;
  std::string tag_;
  std::vector<std::string> keys_;
  std::string trace_context_;
  std::string born_host_;
  std::chrono::system_clock::time_point born_time_{std::chrono::system_clock::now()};
  std::chrono::system_clock::time_point delivery_timestamp_;
  std::string body_;
  std::unordered_map<std::string, std::string> properties_;
  std::string group_;
  Extension extension_;
};

using MessagePtr = std::unique_ptr<Message>;
using MessageConstPtr = std::unique_ptr<const Message>;
using MessageConstSharedPtr = std::shared_ptr<const Message>;
using MessageSharedPtr = std::shared_ptr<Message>;

class MessageBuilder {
public:
  MessageBuilder();

  MessageBuilder& withTopic(std::string topic);

  MessageBuilder& withTag(std::string tag);

  MessageBuilder& withKeys(std::vector<std::string> keys);

  MessageBuilder& withTraceContext(std::string trace_context);

  MessageBuilder& withBody(std::string body);

  MessageBuilder& withGroup(std::string group);

  MessageBuilder& withProperties(std::unordered_map<std::string, std::string> properties);

  /**
   * @brief Specify timepoint after which the message would be available to subscribers.
   *
   * @param delivery_timepoint
   * @return MessageBuilder&
   */
  MessageBuilder& availableAfter(std::chrono::system_clock::time_point delivery_timepoint);

  MessageConstPtr build();

private:
  friend class ClientManagerImpl;

  MessageBuilder& withId(std::string id);

  MessageBuilder& withBornTime(std::chrono::system_clock::time_point born_time);

  MessageBuilder& withBornHost(std::string born_host);

  MessagePtr message_;
};

ROCKETMQ_NAMESPACE_END