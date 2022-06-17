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

#include <memory>
#include <vector>

#include "TopicRouteData.h"
#include "absl/base/thread_annotations.h"
#include "absl/container/flat_hash_set.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/optional.h"

ROCKETMQ_NAMESPACE_BEGIN

class ProducerImpl;

class TopicPublishInfo {
public:
  TopicPublishInfo(std::weak_ptr<ProducerImpl> producer, absl::string_view topic, TopicRouteDataPtr topic_route_data);

  bool selectMessageQueues(absl::optional<std::string> message_group, std::vector<rmq::MessageQueue>& result)
      LOCKS_EXCLUDED(queue_list_mtx_);

  void topicRouteData(TopicRouteDataPtr topic_route_data);

  /**
   * Expose queue list in perspective of message queue list.
   *
   * @return
   */
  std::vector<rmq::MessageQueue> getMessageQueueList() LOCKS_EXCLUDED(queue_list_mtx_);

private:
  std::vector<rmq::MessageQueue> queue_list_ GUARDED_BY(queue_list_mtx_);
  absl::Mutex                                queue_list_mtx_;  // protects message_queue_list_
  std::weak_ptr<ProducerImpl>                producer_;
  std::string                                topic_;
  TopicRouteDataPtr                          topic_route_data_;

  void updatePublishInfo();

  thread_local static uint32_t send_which_queue_;
};

using TopicPublishInfoPtr = std::shared_ptr<TopicPublishInfo>;

ROCKETMQ_NAMESPACE_END