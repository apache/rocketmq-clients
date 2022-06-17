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

#include <cstdint>
#include <string>

#include "google/protobuf/struct.pb.h"
#include "google/protobuf/util/json_util.h"

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

struct QueueData {
  std::string broker_name_;
  std::int32_t read_queue_number_{0};
  std::int32_t write_queue_number_{0};
  std::uint32_t perm_{0};

  /**
   * @brief in Java, it's named "topicSynFlag"
   *
   */
  std::uint32_t topic_system_flag_{0};

  static QueueData decode(const google::protobuf::Struct& root);
};

ROCKETMQ_NAMESPACE_END