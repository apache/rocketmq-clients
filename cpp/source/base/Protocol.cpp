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

#include "Protocol.h"

ROCKETMQ_NAMESPACE_BEGIN

const char* protocolVersion() {
  static const char* protocol_version = "v2";
  return protocol_version;
}

bool writable(rmq::Permission p) {
  switch (p) {
    case rmq::Permission::WRITE:
    case rmq::Permission::READ_WRITE:
      return true;
    default: {
      return false;
    }
  }
}

bool readable(rmq::Permission p) {
  switch (p) {
    case rmq::Permission::READ:
    case rmq::Permission::READ_WRITE:
      return true;
    default:
      return false;
  }
}

bool operator<(const rmq::Resource& lhs, const rmq::Resource& rhs) {
  return lhs.resource_namespace() < rhs.resource_namespace() || lhs.name() < rhs.name();
}

bool operator==(const rmq::Resource& lhs, const rmq::Resource& rhs) {
  return lhs.resource_namespace() == rhs.resource_namespace() && lhs.name() == rhs.name();
}

bool operator<(const rmq::Broker& lhs, const rmq::Broker& rhs) {
  return lhs.name() < rhs.name() || lhs.id() < rhs.id();
}

bool operator==(const rmq::Broker& lhs, const rmq::Broker& rhs) {
  return lhs.name() == rhs.name() && lhs.id() == rhs.id();
}

bool operator<(const rmq::MessageQueue& lhs, const rmq::MessageQueue& rhs) {
  return lhs.topic() < rhs.topic() || lhs.id() < rhs.id() || lhs.broker() < rhs.broker() ||
         lhs.permission() < rhs.permission();
}

bool operator==(const rmq::MessageQueue& lhs, const rmq::MessageQueue& rhs) {
  return lhs.topic() == rhs.topic() && lhs.id() == rhs.id() && lhs.broker() == rhs.broker() &&
         lhs.permission() == rhs.permission();
}

std::string simpleNameOf(const rmq::MessageQueue& m) {
  return fmt::format("{}@{}@{}@{}",
                     m.topic().resource_namespace(), m.topic().name(), m.id(), m.broker().name());
}

bool operator==(const std::vector<rmq::MessageQueue>& lhs, const std::vector<rmq::MessageQueue>& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }

  for (std::size_t i = 0; i < lhs.size(); i++) {
    if (lhs[i] == rhs[i]) {
      continue;
    }
    return false;
  }

  return true;
}

bool operator!=(const std::vector<rmq::MessageQueue>& lhs, const std::vector<rmq::MessageQueue>& rhs) {
  return !(lhs == rhs);
}

std::string urlOf(const rmq::MessageQueue& message_queue) {
  const auto& endpoints = message_queue.broker().endpoints();
  const auto& addresses = endpoints.addresses();
  switch (endpoints.scheme()) {
    case rmq::AddressScheme::DOMAIN_NAME: {
      assert(addresses.size() == 1);
      auto first = addresses.begin();
      return fmt::format("dns:{}:{}", first->host(), first->port());
    }
    case rmq::AddressScheme::IPv4: {
      assert(!addresses.empty());
      auto it = addresses.cbegin();
      std::string result = fmt::format("ipv4:{}:{}", it->host(), it->port());
      for (++it; it != addresses.cend(); ++it) {
        result.append(fmt::format(",{}:{}", it->host(), it->port()));
      }
      return result;
    }
    case rmq::AddressScheme::IPv6: {
      assert(!addresses.empty());
      auto it = addresses.cbegin();
      std::string result = fmt::format("ipv6:{}:{}", it->host(), it->port());
      for (++it; it != addresses.cend(); ++it) {
        result.append(fmt::format(",{}:{}", it->host(), it->port()));
      }
      return result;
    }
    default: {
      break;
    }
  }
  return {};
}

bool operator<(const rmq::Assignment& lhs, const rmq::Assignment& rhs) {
  return lhs.message_queue() < rhs.message_queue();
}

bool operator==(const rmq::Assignment& lhs, const rmq::Assignment& rhs) {
  return lhs.message_queue() == rhs.message_queue();
}

bool operator==(const std::vector<rmq::Assignment>& lhs, const std::vector<rmq::Assignment>& rhs) {
  if (lhs.size() != rhs.size()) {
    return false;
  }

  for (std::size_t i = 0; i < lhs.size(); i++) {
    if (lhs[i] == rhs[i]) {
      continue;
    }
    return false;
  }
  return true;
}

ROCKETMQ_NAMESPACE_END
