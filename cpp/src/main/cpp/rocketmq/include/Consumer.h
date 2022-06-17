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

#include "absl/container/flat_hash_map.h"
#include "absl/types/optional.h"

#include "Client.h"
#include "ConsumeMessageService.h"
#include "FilterExpression.h"

ROCKETMQ_NAMESPACE_BEGIN

class Consumer : virtual public Client {
public:
  ~Consumer() override = default;

  virtual absl::optional<FilterExpression> getFilterExpression(const std::string& topic) const = 0;

  virtual uint32_t maxCachedMessageQuantity() const = 0;

  virtual uint64_t maxCachedMessageMemory() const = 0;

  virtual int32_t receiveBatchSize() const = 0;

  virtual std::shared_ptr<ConsumeMessageService> getConsumeMessageService() = 0;

  virtual bool fifo() const = 0;
};

ROCKETMQ_NAMESPACE_END