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

#include "opencensus/stats/stats.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

class ConsumeStats {
public:
  ConsumeStats();

  const opencensus::stats::MeasureInt64& cachedMessageQuantity() const {
    return cached_message_quantity_;
  }

  const opencensus::stats::MeasureInt64& cachedMessageBytes() const {
    return cached_message_bytes_;
  }

  const opencensus::stats::MeasureInt64& deliveryLatency() const {
    return delivery_latency_;
  }

  const opencensus::stats::MeasureInt64& awaitTime() const {
    return await_time_;
  }

  const opencensus::stats::MeasureInt64& processTime() const {
    return process_time_;
  }

private:
  opencensus::stats::MeasureInt64 cached_message_quantity_;
  opencensus::stats::MeasureInt64 cached_message_bytes_;

  opencensus::stats::MeasureInt64 delivery_latency_;
  opencensus::stats::MeasureInt64 await_time_;
  opencensus::stats::MeasureInt64 process_time_;
};

ROCKETMQ_NAMESPACE_END