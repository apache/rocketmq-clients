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

#include "TopicAssignmentInfo.h"
#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

thread_local uint32_t TopicAssignment::query_which_broker_ = 0;

TopicAssignment::TopicAssignment(const QueryAssignmentResponse& response) {
  if (response.status().code() != rmq::Code::OK) {
    SPDLOG_WARN("QueryAssignmentResponse#code is not SUCCESS. Keep assignment info intact. QueryAssignmentResponse: {}",
                response.DebugString());
    return;
  }

  for (const auto& item : response.assignments()) {
    const rmq::MessageQueue& queue = item.message_queue();
    if (!readable(queue.permission())) {
      continue;
    }

    assert(queue.has_broker());
    const auto& broker = queue.broker();

    if (broker.endpoints().addresses().empty()) {
      SPDLOG_WARN("Broker[{}] is not addressable", broker.DebugString());
      continue;
    }

    rmq::Assignment assignment;
    assignment.mutable_message_queue()->CopyFrom(queue);
    assignment_list_.emplace_back(std::move(assignment));
  }

  std::sort(assignment_list_.begin(), assignment_list_.end(),
            [](const rmq::Assignment& lhs, const rmq::Assignment& rhs) { return lhs < rhs; });
}

bool operator==(const TopicAssignment& lhs, const TopicAssignment& rhs) {
  return lhs.assignmentList() == rhs.assignmentList();
}

ROCKETMQ_NAMESPACE_END