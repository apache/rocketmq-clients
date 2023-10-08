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
#include "NamingScheme.h"

#include <cstdint>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"

ROCKETMQ_NAMESPACE_BEGIN

const char* NamingScheme::DnsPrefix = "dns:";
const char* NamingScheme::IPv4Prefix = "ipv4:";
const char* NamingScheme::IPv6Prefix = "ipv6:";

const char* NamingScheme::IPv4Regex =
    "(([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|[1-9][0-9]|1[0-9][0-9]|2[0-4][0-9]|25[0-5])";

const char* NamingScheme::IPv6Regex = "((([0-9a-fA-F]){1,4})\\:){7}([0-9a-fA-F]){1,4}";

NamingScheme::NamingScheme() : ipv4_pattern_(IPv4Regex), ipv6_pattern_(IPv6Regex) {
}

bool NamingScheme::isIPv4(const std::string& host) {
  return re2::RE2::FullMatch(host, ipv4_pattern_);
}

bool NamingScheme::isIPv6(const std::string& host) {
  return re2::RE2::FullMatch(host, ipv6_pattern_);
}

std::string NamingScheme::buildAddress(const std::vector<std::string>& list) {
  absl::flat_hash_map<std::string, std::uint32_t> ipv4;
  absl::flat_hash_map<std::string, std::uint32_t> ipv6;

  for (const auto& segment : list) {
    std::vector<std::string> host_port = absl::StrSplit(segment, ':');
    if (2 != host_port.size()) {
      continue;
    }

    if (isIPv4(host_port[0])) {
      std::uint32_t port;
      if (absl::SimpleAtoi(host_port[1], &port)) {
        ipv4.insert_or_assign(host_port[0], port);
      }
      continue;
    }

    if (isIPv6(host_port[0])) {
      std::uint32_t port;
      if (absl::SimpleAtoi(host_port[1], &port)) {
        ipv6.insert_or_assign(host_port[0], port);
      }
      continue;
    }

    // Once we find a domain name record, use it as the final result.
    host_port.insert(host_port.begin(), "dns");
    return absl::StrJoin(host_port, ":");
  }

  if (!ipv4.empty()) {
    return "ipv4:" + absl::StrJoin(ipv4, ",", absl::PairFormatter(":"));
  }

  if (!ipv6.empty()) {
    return "ipv6:" + absl::StrJoin(ipv4, ",", absl::PairFormatter(":"));
  }
  return {};
}

bool NamingScheme::accept(absl::string_view endpoint) {
  if (absl::StartsWith(endpoint, DnsPrefix)) {
    return true;
  }

  if (absl::StartsWith(endpoint, IPv4Prefix)) {
    return true;
  }
  
  if (absl::StartsWith(endpoint, IPv6Prefix)) {
    return true;
  }

  return false;
}

ROCKETMQ_NAMESPACE_END