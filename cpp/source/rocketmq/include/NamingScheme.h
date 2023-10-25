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
#include <vector>

#include "absl/strings/string_view.h"
#include "re2/re2.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class NamingScheme {
public:
  NamingScheme();

  std::string buildAddress(const std::vector<std::string>& list);

  bool accept(absl::string_view endpoint);

  static const char* DnsPrefix;
  static const char* IPv4Prefix;
  static const char* IPv6Prefix;

private:
  static const char* IPv4Regex;
  static const char* IPv6Regex;

  bool isIPv4(const std::string& host);

  bool isIPv6(const std::string& host);

  re2::RE2 ipv4_pattern_;
  re2::RE2 ipv6_pattern_;
};

ROCKETMQ_NAMESPACE_END