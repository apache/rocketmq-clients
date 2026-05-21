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
#include "TopicPublishInfo.h"

#include <memory>
#include <utility>

#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"
#include "MixAll.h"
#include "ProducerImpl.h"
#include "TopicRouteData.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_join.h"

ROCKETMQ_NAMESPACE_BEGIN

thread_local uint32_t TopicPublishInfo::send_which_queue_ = MixAll::random(0, 100);

TopicPublishInfo::TopicPublishInfo(std::weak_ptr<ProducerImpl> producer,
                                   absl::string_view           topic,
                                   TopicRouteDataPtr           topic_route_data)
    : producer_(std::move(producer)),
      topic_(topic.data(), topic.length()),
      topic_route_data_(std::move(topic_route_data)) {
  updatePublishInfo();
}

bool TopicPublishInfo::selectMessageQueues(absl::optional<std::string>     message_group,
                                           std::vector<rmq::MessageQueue>& result) {
  {
    absl::MutexLock lock(&queue_list_mtx_);
    if (queue_list_.empty()) {
      SPDLOG_WARN("Failed to screen viable message queues from TopicPublishInfo because there is no candidates at all");
      return false;
    }
  }

  auto producer = producer_.lock();
  if (!producer) {
    return false;
  }

  // Select a message queue according to given message group, which is used to send FIFO message.
  if (message_group.has_value()) {
    std::size_t     hash_code = std::hash<std::string>{}(message_group.value());
    absl::MutexLock lk(&queue_list_mtx_);
    std::size_t     index = hash_code % queue_list_.size();
    result.push_back(queue_list_[index]);
    return true;
  }

  unsigned int index = ++send_which_queue_;
  // For standard messages, select at most "count" message queues in case retries are required.
  {
    absl::MutexLock lock(&queue_list_mtx_);
    for (std::vector<rmq::MessageQueue>::size_type i = 0; i < queue_list_.size(); ++i) {
      const rmq::MessageQueue& message_queue = queue_list_[index++ % (queue_list_.size())];
      if (!writable(message_queue.permission())) {
        continue;
      }

      if (!producer->isEndpointIsolated(urlOf(message_queue))) {
        auto search = std::find_if(
            result.begin(), result.end(), [&](const rmq::MessageQueue& item) {
          return item.broker().name() == message_queue.broker().name();
        });
        if (std::end(result) == search) {
          result.emplace_back(message_queue);
        }
        if (result.size() >= producer->maxAttemptTimes()) {
          return true;
        }
      }
    }
  }
  return !result.empty();
}

void TopicPublishInfo::updatePublishInfo() {
  std::vector<rmq::MessageQueue> writable_queue_list;
  {
    for (const auto& queue : topic_route_data_->messageQueues()) {
      assert(queue.topic().name() == topic_);
      if (!writable(queue.permission())) {
        continue;
      }
      if (MixAll::MASTER_BROKER_ID != queue.broker().id()) {
        continue;
      }
      writable_queue_list.push_back(queue);
    }
  }

  if (writable_queue_list.empty()) {
    SPDLOG_WARN("No writable queue is current available. Skip updating publish table for topic={}", topic_);
    return;
  }

  {
    absl::MutexLock lk(&queue_list_mtx_);
    queue_list_.swap(writable_queue_list);
  }
}

void TopicPublishInfo::topicRouteData(TopicRouteDataPtr topic_route_data) {
  SPDLOG_DEBUG("Update publish info according to renewed route data of topic={}", topic_);
  topic_route_data_ = std::move(topic_route_data);
  updatePublishInfo();
}

std::vector<rmq::MessageQueue> TopicPublishInfo::getMessageQueueList() {
  absl::MutexLock lock(&queue_list_mtx_);
  return queue_list_;
}

ROCKETMQ_NAMESPACE_END