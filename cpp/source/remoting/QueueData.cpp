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
#include "QueueData.h"

ROCKETMQ_NAMESPACE_BEGIN

QueueData QueueData::decode(const google::protobuf::Struct& root) {
  auto fields = root.fields();

  QueueData queue_data;

  if (fields.contains("brokerName")) {
    queue_data.broker_name_ = fields["brokerName"].string_value();
  }

  if (fields.contains("readQueueNums")) {
    queue_data.read_queue_number_ = fields["readQueueNums"].number_value();
  }

  if (fields.contains("writeQueueNums")) {
    queue_data.write_queue_number_ = fields["writeQueueNums"].number_value();
  }

  if (fields.contains("perm")) {
    queue_data.perm_ = fields["perm"].number_value();
  }

  if (fields.contains("topicSynFlag")) {
    queue_data.topic_system_flag_ = fields["topicSynFlag"].number_value();
  }

  return queue_data;
}

ROCKETMQ_NAMESPACE_END