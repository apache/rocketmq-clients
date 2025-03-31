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

#include <functional>
#include <system_error>

#include "ReceiveMessageResult.h"

ROCKETMQ_NAMESPACE_BEGIN

class ProcessQueue;

class AsyncReceiveMessageCallback : public std::enable_shared_from_this<AsyncReceiveMessageCallback> {
public:
  explicit AsyncReceiveMessageCallback(std::weak_ptr<ProcessQueue> process_queue);

  void onCompletion(const std::error_code& ec, std::string& attempt_id, const ReceiveMessageResult& result);

  void receiveMessageLater(std::chrono::milliseconds delay, std::string& attempt_id);

  void receiveMessageImmediately(std::string& attempt_id);

private:
  /**
   * Hold a weak_ptr to ProcessQueue. Once ProcessQueue was released, stop the
   * pop-cycle immediately.
   */
  std::weak_ptr<ProcessQueue> process_queue_;

  std::function<void(std::string)> receive_message_later_;

  void checkThrottleThenReceive(std::string attempt_id);

  static const char* RECEIVE_LATER_TASK_NAME;
};

ROCKETMQ_NAMESPACE_END