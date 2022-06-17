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
#include "TopicRouteData.h"
#include "BrokerData.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

TopicRouteData TopicRouteData::decode(const google::protobuf::Struct& root) {
  auto fields = root.fields();

  TopicRouteData topic_route_data;

  if (fields.contains("queueDatas")) {
    auto queue_data_list = fields.at("queueDatas");

    for (auto& item : queue_data_list.list_value().values()) {
      topic_route_data.queue_data_.push_back(QueueData::decode(item.struct_value()));
    }
  }

  if (fields.contains("brokerDatas")) {
    auto broker_data_list = fields.at("brokerDatas");
    for (auto& item : broker_data_list.list_value().values()) {
      topic_route_data.broker_data_.push_back(BrokerData::decode(item.struct_value()));
    }
  }

  return topic_route_data;
}

ROCKETMQ_NAMESPACE_END