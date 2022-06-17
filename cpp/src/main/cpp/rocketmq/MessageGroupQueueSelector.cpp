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
#include "MessageGroupQueueSelector.h"

#include <cassert>
#include <utility>

ROCKETMQ_NAMESPACE_BEGIN

MessageGroupQueueSelector::MessageGroupQueueSelector(std::string message_group)
    : message_group_(std::move(message_group)) {
}

rmq::MessageQueue MessageGroupQueueSelector::select(const std::vector<rmq::MessageQueue>& mqs) {
  std::size_t hash_code = std::hash<std::string>{}(message_group_);
  assert(!mqs.empty());
  std::size_t len = mqs.size();
  return mqs[hash_code % len];
}

ROCKETMQ_NAMESPACE_END