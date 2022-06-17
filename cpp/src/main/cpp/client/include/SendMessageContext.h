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

#include "Protocol.h"
#include "rocketmq/Message.h"

ROCKETMQ_NAMESPACE_BEGIN
class SendMessageContext {
public:
  const std::string& getProducerGroup() const {
    return producer_group_;
  };
  void setProducerGroup(const std::string& producer_group) {
    this->producer_group_ = producer_group;
  };
  const Message& getMessage() const {
    return message_;
  }
  void setMessage(const Message& message) {
    this->message_ = message;
  }
  const rmq::MessageQueue& getMessageQueue() const {
    return message_queue_;
  }
  void setMessageQueue(const rmq::MessageQueue& message_queue) {
    this->message_queue_ = message_queue;
  }
  const std::string& getBornHost() const {
    return born_host_;
  }
  void setBornHost(const std::string& born_host) {
    this->born_host_ = born_host;
  }
  const std::string& getMessageId() const {
    return message_id_;
  }
  void setMessageId(const std::string& message_id) {
    this->message_id_ = message_id;
  }
  long long int getQueueOffset() const {
    return queue_offset_;
  }
  void setQueueOffset(long long queue_offset) {
    this->queue_offset_ = queue_offset;
  }

private:
  std::string producer_group_;
  Message message_;
  rmq::MessageQueue message_queue_;
  std::string born_host_;
  std::string message_id_;
  long long queue_offset_{-1};
};

ROCKETMQ_NAMESPACE_END