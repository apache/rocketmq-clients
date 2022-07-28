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

#include "ConsumeMessageService.h"
#include "rocketmq/Message.h"

ROCKETMQ_NAMESPACE_BEGIN

class ProcessQueue;

/**
 * @brief Operation to take for the consume-task.
 */
enum class NextStep : std::uint8_t
{
  /**
   * @brief Continue to consume the remaining messages.
   */
  Consume = 0,

  /**
   * @brief Ack the head, aka, messages_[0].
   */
  Ack,

  Nack,

  /**
   * @brief Forward the head, aka, messages_[0], to dead-letter-queue.
   */
  Forward,
};

class ConsumeTask : public std::enable_shared_from_this<ConsumeTask> {
public:
  ConsumeTask(ConsumeMessageServiceWeakPtr service,
              std::weak_ptr<ProcessQueue> process_queue,
              MessageConstSharedPtr message);

  ConsumeTask(ConsumeMessageServiceWeakPtr service,
              std::weak_ptr<ProcessQueue> process_queue,
              std::vector<MessageConstSharedPtr> messages);

  void process();

  void submit();

  void schedule();

private:
  ConsumeMessageServiceWeakPtr service_;
  std::weak_ptr<ProcessQueue> process_queue_;
  std::vector<MessageConstSharedPtr> messages_;
  bool fifo_{false};
  NextStep next_step_{NextStep::Consume};

  /**
   * @brief messages_[0] has completed its life-cycle.
   */
  void pop();

  static void onAck(std::shared_ptr<ConsumeTask> task, const std::error_code& ec);

  static void onNack(std::shared_ptr<ConsumeTask> task, const std::error_code& ec);

  static void onForward(std::shared_ptr<ConsumeTask> task, const std::error_code& ec);
};

using ConsumeTaskSharedPtr = std::shared_ptr<ConsumeTask>;

ROCKETMQ_NAMESPACE_END