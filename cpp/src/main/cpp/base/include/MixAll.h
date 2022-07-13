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

#include <chrono>
#include <cstdint>
#include <string>
#include <system_error>

#include "absl/strings/string_view.h"
#include "re2/re2.h"

#include "rocketmq/Message.h"

ROCKETMQ_NAMESPACE_BEGIN

class MixAll {
public:
  static const int32_t MASTER_BROKER_ID;
  static const int32_t DEFAULT_RECEIVE_MESSAGE_BATCH_SIZE;
  static const uint32_t MAX_MESSAGE_BODY_SIZE;
  static const uint32_t MAX_CACHED_MESSAGE_COUNT;
  static const uint32_t DEFAULT_CACHED_MESSAGE_COUNT;
  static const uint64_t DEFAULT_CACHED_MESSAGE_MEMORY;
  static const uint32_t DEFAULT_CONSUME_THREAD_POOL_SIZE;
  static const uint32_t DEFAULT_CONSUME_MESSAGE_BATCH_SIZE;
  static const int32_t DEFAULT_MAX_DELIVERY_ATTEMPTS;

  static const RE2 TOPIC_REGEX;
  static const RE2 IP_REGEX;

  /**
   * The amount of time required before a popped message is eligible to be consumed again. By default, 30s.
   */
  static const std::chrono::duration<long long> DEFAULT_INVISIBLE_TIME_;

  static const std::chrono::duration<long long> PROCESS_QUEUE_EXPIRATION_THRESHOLD_;

  static const std::chrono::milliseconds DefaultReceiveMessageTimeout;

  static const std::size_t MAX_SEND_MESSAGE_ATTEMPT_TIMES_;

  static const std::string PROPERTY_TRANSACTION_PREPARED_;

  static const std::string DEFAULT_LOAD_BALANCER_STRATEGY_NAME_;

  static const uint32_t DEFAULT_COMPRESS_BODY_THRESHOLD_;

  static const char* HOME_PROFILE_ENV_;

  static const char* MESSAGE_KEY_SEPARATOR;

  static const char* OTLP_NAME_VALUE;

  static const char* TRACE_RESOURCE_ATTRIBUTE_KEY_TELEMETRY_SDK_LANGUAGE;
  static const char* TRACE_RESOURCE_ATTRIBUTE_VALUE_TELEMETRY_SDK_LANGUAGE;

  static const char* TRACE_RESOURCE_ATTRIBUTE_KEY_HOST_NAME;
  static const char* TRACE_RESOURCE_ATTRIBUTE_KEY_SERVICE_NAME;
  static const char* TRACE_RESOURCE_ATTRIBUTE_VALUE_SERVICE_NAME;

  // RocketMQ span attribute name list
  static const char* SPAN_ATTRIBUTE_KEY_ROCKETMQ_OPERATION;
  static const char* SPAN_ATTRIBUTE_KEY_ROCKETMQ_NAMESPACE;
  static const char* SPAN_ATTRIBUTE_KEY_ROCKETMQ_TAG;
  static const char* SPAN_ATTRIBUTE_KEY_ROCKETMQ_KEYS;
  static const char* SPAN_ATTRIBUTE_KEY_ROCKETMQ_CLIENT_ID;
  static const char* SPAN_ATTRIBUTE_KEY_ROCKETMQ_MESSAGE_TYPE;
  static const char* SPAN_ATTRIBUTE_KEY_ROCKETMQ_CLIENT_GROUP;
  static const char* SPAN_ATTRIBUTE_KEY_ROCKETMQ_ATTEMPT;
  static const char* SPAN_ATTRIBUTE_KEY_ROCKETMQ_BATCH_SIZE;
  static const char* SPAN_ATTRIBUTE_KEY_ROCKETMQ_DELIVERY_TIMESTAMP;
  static const char* SPAN_ATTRIBUTE_KEY_ROCKETMQ_AVAILABLE_TIMESTAMP;
  static const char* SPAN_ATTRIBUTE_KEY_ROCKETMQ_ACCESS_KEY;

  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_MESSAGING_SYSTEM;
  static const char* SPAN_ATTRIBUTE_VALUE_DESTINATION_KIND;
  static const char* SPAN_ATTRIBUTE_VALUE_MESSAGING_PROTOCOL;
  static const char* SPAN_ATTRIBUTE_VALUE_MESSAGING_PROTOCOL_VERSION;

  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_NORMAL_MESSAGE;
  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_FIFO_MESSAGE;
  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_DELAY_MESSAGE;
  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_TRANSACTION_MESSAGE;

  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_SEND_OPERATION;
  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_RECEIVE_OPERATION;
  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_PULL_OPERATION;
  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_AWAIT_OPERATION;
  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_PROCESS_OPERATION;
  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_ACK_OPERATION;
  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_NACK_OPERATION;
  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_COMMIT_OPERATION;
  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_ROLLBACK_OPERATION;
  static const char* SPAN_ATTRIBUTE_VALUE_ROCKETMQ_DLQ_OPERATION;

  // Messaging attribute name list
  static const char* SPAN_ATTRIBUTE_KEY_MESSAGING_SYSTEM;
  static const char* SPAN_ATTRIBUTE_KEY_MESSAGING_DESTINATION;
  static const char* SPAN_ATTRIBUTE_KEY_MESSAGING_DESTINATION_KIND;
  static const char* SPAN_ATTRIBUTE_KEY_MESSAGING_PROTOCOL;
  static const char* SPAN_ATTRIBUTE_KEY_MESSAGING_PROTOCOL_VERSION;
  static const char* SPAN_ATTRIBUTE_KEY_MESSAGING_URL;
  static const char* SPAN_ATTRIBUTE_KEY_MESSAGING_ID;
  static const char* SPAN_ATTRIBUTE_KEY_MESSAGING_PAYLOAD_SIZE_BYTES;
  static const char* SPAN_ATTRIBUTE_KEY_MESSAGING_OPERATION;

  static const char* SPAN_ATTRIBUTE_VALUE_MESSAGING_SEND_OPERATION;
  static const char* SPAN_ATTRIBUTE_VALUE_MESSAGING_RECEIVE_OPERATION;
  static const char* SPAN_ATTRIBUTE_VALUE_MESSAGING_PROCESS_OPERATION;

  static const char* SPAN_ATTRIBUTE_KEY_TRANSACTION_RESOLUTION;

  // Tracing annotation
  static const char* SPAN_ANNOTATION_AWAIT_CONSUMPTION;
  static const char* SPAN_ANNOTATION_MESSAGE_KEYS;
  static const char* SPAN_ANNOTATION_ATTR_START_TIME;

  template <typename Rep, typename Period>
  static int64_t millisecondsOf(std::chrono::duration<Rep, Period> duration) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(duration).count();
  }

  template <typename Rep, typename Period>
  static int64_t microsecondsOf(std::chrono::duration<Rep, Period> duration) {
    return std::chrono::duration_cast<std::chrono::microseconds>(duration).count();
  }

  /**
   * Validate message is legal. Aka, topic
   * @param message
   * @return
   */
  static void validate(const Message& message, std::error_code& ec);

  static uint32_t random(uint32_t left, uint32_t right);

  static bool crc32(const std::string& data, std::string& digest);

  static bool md5(const std::string& data, std::string& digest);

  static bool sha1(const std::string& data, std::string& digest);

  static std::string format(std::chrono::system_clock::time_point time_point);

  static std::string hex(const void* data, std::size_t len);

  static bool hexToBinary(const std::string& hex, std::vector<uint8_t>& bin);

  static bool homeDirectory(std::string& home);

  static bool isIPv4(absl::string_view host);

  static const char* osName();

private:
  static bool hexCharValue(char c, uint8_t& value);
};

ROCKETMQ_NAMESPACE_END