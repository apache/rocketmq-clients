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
#include "MixAll.h"

#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <system_error>

#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"
#include "absl/random/random.h"
#include "absl/strings/str_split.h"
#include "fmt/format.h"
#include "openssl/md5.h"
#include "openssl/sha.h"
#include "rocketmq/ErrorCode.h"
#include "zlib.h"

#ifdef _WIN32
#include <winsock.h>
#else
#include <arpa/inet.h>
#include <pwd.h>
#include <unistd.h>
#endif

ROCKETMQ_NAMESPACE_BEGIN

const int32_t MixAll::MASTER_BROKER_ID = 0;

const int32_t MixAll::DEFAULT_RECEIVE_MESSAGE_BATCH_SIZE = 32;

const uint32_t MixAll::MAX_MESSAGE_BODY_SIZE = 1024 * 1024 * 4;
const uint32_t MixAll::MAX_CACHED_MESSAGE_COUNT = 65535;
const uint32_t MixAll::DEFAULT_CACHED_MESSAGE_COUNT = 1024;
const uint64_t MixAll::DEFAULT_CACHED_MESSAGE_MEMORY = 128L * 1024 * 1024;
const uint32_t MixAll::DEFAULT_CONSUME_THREAD_POOL_SIZE = 20;
const uint32_t MixAll::DEFAULT_CONSUME_MESSAGE_BATCH_SIZE = 1;
const int32_t MixAll::DEFAULT_MAX_DELIVERY_ATTEMPTS = 16;

const RE2 MixAll::TOPIC_REGEX("[a-zA-Z0-9\\-_%]{1,64}");
const RE2 MixAll::IP_REGEX("\\d+\\.\\d+\\.\\d+\\.\\d+");

const std::chrono::duration<long long> MixAll::DEFAULT_INVISIBLE_TIME_ = std::chrono::seconds(30);

const std::chrono::duration<long long> MixAll::PROCESS_QUEUE_EXPIRATION_THRESHOLD_ = std::chrono::seconds(120);

const std::size_t MixAll::MAX_SEND_MESSAGE_ATTEMPT_TIMES_ = 3;

const std::string MixAll::PROPERTY_TRANSACTION_PREPARED_ = "TRAN_MSG";

const std::string MixAll::DEFAULT_LOAD_BALANCER_STRATEGY_NAME_ = "AVG";

const uint32_t MixAll::DEFAULT_COMPRESS_BODY_THRESHOLD_ = 1024 * 1024 * 4;

const std::chrono::milliseconds MixAll::DefaultReceiveMessageTimeout = std::chrono::seconds(30);

const char* MixAll::HOME_PROFILE_ENV_ = "HOME";
const char* MixAll::MESSAGE_KEY_SEPARATOR = " ";

const char* MixAll::OTLP_NAME_VALUE = "org.apache.rocketmq.message";

const char* MixAll::TRACE_RESOURCE_ATTRIBUTE_KEY_TELEMETRY_SDK_LANGUAGE = "telemetry.sdk.language";
const char* MixAll::TRACE_RESOURCE_ATTRIBUTE_VALUE_TELEMETRY_SDK_LANGUAGE = "cpp";

const char* MixAll::TRACE_RESOURCE_ATTRIBUTE_KEY_HOST_NAME = "host.name";
const char* MixAll::TRACE_RESOURCE_ATTRIBUTE_KEY_SERVICE_NAME = "service.name";
const char* MixAll::TRACE_RESOURCE_ATTRIBUTE_VALUE_SERVICE_NAME = "rocketmq-client";

// Span attributes follows to the opentelemetry specification, refers to:
// https://github.com/open-telemetry/opentelemetry-specification

// RocketMQ span attribute name list
const char* MixAll::SPAN_ATTRIBUTE_KEY_ROCKETMQ_OPERATION = "messaging.rocketmq.operation";
const char* MixAll::SPAN_ATTRIBUTE_KEY_ROCKETMQ_NAMESPACE = "messaging.rocketmq.namespace";
const char* MixAll::SPAN_ATTRIBUTE_KEY_ROCKETMQ_TAG = "messaging.rocketmq.message_tag";
const char* MixAll::SPAN_ATTRIBUTE_KEY_ROCKETMQ_KEYS = "messaging.rocketmq.message_keys";
const char* MixAll::SPAN_ATTRIBUTE_KEY_ROCKETMQ_CLIENT_ID = "messaging.rocketmq.client_id";
const char* MixAll::SPAN_ATTRIBUTE_KEY_ROCKETMQ_MESSAGE_TYPE = "messaging.rocketmq.message_type";
const char* MixAll::SPAN_ATTRIBUTE_KEY_ROCKETMQ_CLIENT_GROUP = "messaging.rocketmq.client_group";
const char* MixAll::SPAN_ATTRIBUTE_KEY_ROCKETMQ_ATTEMPT = "messaging.rocketmq.attempt";
const char* MixAll::SPAN_ATTRIBUTE_KEY_ROCKETMQ_BATCH_SIZE = "messaging.rocketmq.batch_size";
const char* MixAll::SPAN_ATTRIBUTE_KEY_ROCKETMQ_DELIVERY_TIMESTAMP = "messaging.rocketmq.delivery_timestamp";
const char* MixAll::SPAN_ATTRIBUTE_KEY_ROCKETMQ_AVAILABLE_TIMESTAMP = "messaging.rocketmq.available_timestamp";
const char* MixAll::SPAN_ATTRIBUTE_KEY_ROCKETMQ_ACCESS_KEY = "messaging.rocketmq.access_key";

const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_MESSAGING_SYSTEM = "rocketmq";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_DESTINATION_KIND = "topic";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_MESSAGING_PROTOCOL = "RMQ-gRPC";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_MESSAGING_PROTOCOL_VERSION = "v2";

const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_NORMAL_MESSAGE = "normal";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_FIFO_MESSAGE = "fifo";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_DELAY_MESSAGE = "delay";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_TRANSACTION_MESSAGE = "transaction";

const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_SEND_OPERATION = "send";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_RECEIVE_OPERATION = "receive";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_PULL_OPERATION = "pull";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_AWAIT_OPERATION = "await";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_PROCESS_OPERATION = "process";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_ACK_OPERATION = "ack";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_NACK_OPERATION = "nack";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_COMMIT_OPERATION = "commit";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_ROLLBACK_OPERATION = "rollback";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_ROCKETMQ_DLQ_OPERATION = "dlq";

// Messaging span attribute name list
const char* MixAll::SPAN_ATTRIBUTE_KEY_MESSAGING_SYSTEM = "messaging.system";
const char* MixAll::SPAN_ATTRIBUTE_KEY_MESSAGING_DESTINATION = "messaging.destination";
const char* MixAll::SPAN_ATTRIBUTE_KEY_MESSAGING_DESTINATION_KIND = "messaging.destination_kind";
const char* MixAll::SPAN_ATTRIBUTE_KEY_MESSAGING_PROTOCOL = "messaging.protocol";
const char* MixAll::SPAN_ATTRIBUTE_KEY_MESSAGING_PROTOCOL_VERSION = "messaging.protocol_version";
const char* MixAll::SPAN_ATTRIBUTE_KEY_MESSAGING_URL = "messaging.url";
const char* MixAll::SPAN_ATTRIBUTE_KEY_MESSAGING_ID = "messaging.message_id";
const char* MixAll::SPAN_ATTRIBUTE_KEY_MESSAGING_PAYLOAD_SIZE_BYTES = "messaging.message_payload_size_bytes";
const char* MixAll::SPAN_ATTRIBUTE_KEY_MESSAGING_OPERATION = "messaging.operation";

const char* MixAll::SPAN_ATTRIBUTE_VALUE_MESSAGING_SEND_OPERATION = "send";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_MESSAGING_RECEIVE_OPERATION = "receive";
const char* MixAll::SPAN_ATTRIBUTE_VALUE_MESSAGING_PROCESS_OPERATION = "process";

const char* MixAll::SPAN_ATTRIBUTE_KEY_TRANSACTION_RESOLUTION = "commitAction";

// Span annotation
const char* MixAll::SPAN_ANNOTATION_AWAIT_CONSUMPTION = "__await_consumption";
const char* MixAll::SPAN_ANNOTATION_MESSAGE_KEYS = "__message_keys";
const char* MixAll::SPAN_ANNOTATION_ATTR_START_TIME = "__start_time";

void MixAll::validate(const Message& message, std::error_code& ec) {
  if (message.topic().empty()) {
    SPDLOG_WARN("Topic of the message to publish is empty");
    ec = ErrorCode::IllegalTopic;
    return;
  }

  const std::string& topic = message.topic();

  // Legal topic characters are a-z, A-Z, 0-9, hyphen('-') and underline('_')
  if (!RE2::FullMatch(topic, TOPIC_REGEX)) {
    SPDLOG_WARN("Topic of the message to publish does not match [a-zA-Z0-9\\-_%]{1,64}");
    ec = ErrorCode::IllegalTopic;
  }
}

uint32_t MixAll::random(uint32_t left, uint32_t right) {
  static absl::BitGen gen;
  return absl::Uniform(gen, left, right);
}

bool MixAll::crc32(const std::string& data, std::string& digest) {
  uLong crc = ::crc32(0L, reinterpret_cast<const Bytef*>(data.c_str()), data.length());
  uint32_t network_byte_order = htonl(crc);
  digest = hex(&network_byte_order, sizeof(network_byte_order));
  return true;
}

bool MixAll::md5(const std::string& data, std::string& digest) {
  MD5_CTX ctx;
  MD5_Init(&ctx);
  MD5_Update(&ctx, data.data(), data.length());
  unsigned char md[MD5_DIGEST_LENGTH + 1];
  int success = MD5_Final(md, &ctx);
  if (!success) {
    return false;
  }
  digest.clear();
  digest.append(hex(md, MD5_DIGEST_LENGTH));
  return true;
}

bool MixAll::sha1(const std::string& data, std::string& digest) {
  unsigned char out[SHA_DIGEST_LENGTH];
  SHA_CTX ctx;
  SHA1_Init(&ctx);
  SHA1_Update(&ctx, data.data(), data.length());
  SHA1_Final(out, &ctx);
  digest.clear();
  digest.append(hex(reinterpret_cast<const char*>(out), SHA_DIGEST_LENGTH));
  return true;
}

std::string MixAll::format(std::chrono::system_clock::time_point time_point) {
  std::time_t creation_time_t = std::chrono::system_clock::to_time_t(time_point);
  auto fraction = std::chrono::duration_cast<std::chrono::milliseconds>(time_point.time_since_epoch()).count() % 1000;
  char fmt_date_time[128];

  /**
   * TODO: std::localtime is not thread-safe, output, as a result, may be less reliable in highly contending
   * scenario
   */
  std::strftime(fmt_date_time, sizeof(fmt_date_time), "%Y-%m-%d %H:%M:%S", std::localtime(&creation_time_t));
  return fmt::format("{}.{}", fmt_date_time, fraction);
}

std::string MixAll::hex(const void* data, std::size_t len) {
  const char dict[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};
  std::string s;
  const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);
  for (std::size_t i = 0; i < len; i++) {
    unsigned char c = *(ptr + i);
    s.append(&dict[(0xF0 & c) >> 4], 1);
    s.append(&dict[(0x0F & c)], 1);
  }
  return s;
}

bool MixAll::hexToBinary(const std::string& hex, std::vector<uint8_t>& bin) {
  // Length of valid Hex string should always be even.
  if (hex.length() % 2) {
    return false;
  }
  for (std::string::size_type i = 0; i < hex.length(); i += 2) {
    char c1 = hex.at(i);
    char c2 = hex.at(i + 1);

    uint8_t value = 0;
    uint8_t tmp;
    if (hexCharValue(c1, tmp)) {
      value = tmp << 4;
    } else {
      return false;
    }

    if (hexCharValue(c2, tmp)) {
      value |= tmp;
      bin.push_back(value);
    } else {
      return false;
    }
  }
  return true;
}

bool MixAll::hexCharValue(char c, uint8_t& value) {
  if ('0' <= c && c <= '9') {
    value = c - '0';
    return true;
  }

  if ('a' <= c && c <= 'f') {
    value = c - 'a' + 10;
    return true;
  }

  if ('A' <= c && c <= 'F') {
    value = c - 'A' + 10;
    return true;
  }

  return false;
}

bool MixAll::homeDirectory(std::string& home_dir) {
#ifndef _WIN32
  char* home = getenv(HOME_PROFILE_ENV_);
  if (home) {
    home_dir.append(home, strlen(home));
    return true;
  } else {
    struct passwd* pwd = getpwuid(getuid());
    if (pwd) {
      home_dir.clear();
      home_dir.append(pwd->pw_dir, strlen(pwd->pw_dir));
      return true;
    }
  }
  return false;
#else
  char* home = getenv("USERPROFILE");
  if (home) {
    home_dir.clear();
    home_dir.append(home, strlen(home));
    return true;
  }
  return false;
#endif
}

bool MixAll::isIPv4(absl::string_view host) {
  return RE2::FullMatch(re2::StringPiece(host.data(), host.length()), IP_REGEX);
}

const char* MixAll::osName() {
#if INTPTR_MAX == INT32_MAX
#define BITS " 32-bit"
#elif INTPTR_MAX == INT64_MAX
#define BITS " 64-bit"
#else
#define BITS ""
#endif

#if defined(_WIN32) || defined(_WIN64)
  return "Windows" BITS;
#elif __APPLE__ || __MACH__
  return "Mac OSX" BITS;
#elif __linux__
  return "Linux" BITS;
#elif __FreeBSD__
  return "FreeBSD" BITS;
#elif __unix || __unix__
  return "Unix" BITS;
#else
  return "Other" BITS;
#endif
}

ROCKETMQ_NAMESPACE_END