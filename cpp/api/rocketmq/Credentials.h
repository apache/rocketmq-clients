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
#include <string>
#include <utility>

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class Credentials {
public:
  Credentials() : expiration_instant_(std::chrono::system_clock::time_point::max()) {
  }

  Credentials(std::string access_key, std::string access_secret)
      : access_key_(std::move(access_key)), access_secret_(std::move(access_secret)),
        expiration_instant_(std::chrono::system_clock::time_point::max()) {
  }

  Credentials(std::string access_key, std::string access_secret, std::string session_token,
              std::chrono::system_clock::time_point expiration)
      : access_key_(std::move(access_key)), access_secret_(std::move(access_secret)),
        session_token_(std::move(session_token)), expiration_instant_(expiration) {
  }

  bool operator==(const Credentials& rhs) const {
    return access_key_ == rhs.access_key_ && access_secret_ == rhs.access_secret_ &&
           session_token_ == rhs.session_token_ && expiration_instant_ == rhs.expiration_instant_;
  }

  bool operator!=(const Credentials& rhs) const {
    return !(*this == rhs);
  }

  bool empty() const {
    return access_key_.empty() || access_secret_.empty();
  }

  bool expired() const {
    return std::chrono::system_clock::now() > expiration_instant_;
  }

  const std::string& accessKey() const {
    return access_key_;
  }

  const std::string& accessSecret() const {
    return access_secret_;
  }

  const std::string& sessionToken() const {
    return session_token_;
  }

  std::chrono::system_clock::time_point expirationInstant() const {
    return expiration_instant_;
  }

private:
  std::string access_key_;
  std::string access_secret_;
  std::string session_token_;
  std::chrono::system_clock::time_point expiration_instant_;
};

ROCKETMQ_NAMESPACE_END