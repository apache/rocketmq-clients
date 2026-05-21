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

#include <algorithm>
#include <apache/rocketmq/v2/definition.pb.h>
#include <string>
#include <vector>

#include "absl/strings/str_join.h"

#include "Protocol.h"

ROCKETMQ_NAMESPACE_BEGIN

/**
 * Thread Safety: This class is immutable and thus effectively thread safe.
 */
class TopicRouteData {
public:
  TopicRouteData(std::vector<rmq::MessageQueue> message_queues) : message_queues_(message_queues) {
    std::sort(message_queues_.begin(), message_queues_.end(),
              [](const rmq::MessageQueue& lhs, const rmq::MessageQueue& rhs) { return lhs < rhs; });
  }

  const std::vector<rmq::MessageQueue>& messageQueues() const {
    return message_queues_;
  }

  std::string debugString() const {
    return absl::StrJoin(message_queues_.begin(), message_queues_.end(), ",",
                         [](std::string* out, const rmq::MessageQueue& m) { out->append(m.ShortDebugString()); });
  };

private:
  std::vector<rmq::MessageQueue> message_queues_;

  friend bool operator==(const TopicRouteData&, const TopicRouteData&);

  friend bool operator!=(const TopicRouteData&, const TopicRouteData&);
};

inline bool operator==(const TopicRouteData& lhs, const TopicRouteData& rhs) {
  return lhs.message_queues_ == rhs.message_queues_;
}

inline bool operator!=(const TopicRouteData& lhs, const TopicRouteData& rhs) {
  return lhs.message_queues_ != rhs.message_queues_;
}

using TopicRouteDataPtr = std::shared_ptr<TopicRouteData>;

ROCKETMQ_NAMESPACE_END