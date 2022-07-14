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

#include "ConsumeStats.h"

#include "Tag.h"

ROCKETMQ_NAMESPACE_BEGIN

ConsumeStats::ConsumeStats()
    : cached_message_quantity_(opencensus::stats::MeasureInt64::Register(
          "cached_message_quantity", "Number of locally cached messages", "1")),
      cached_message_bytes_(opencensus::stats::MeasureInt64::Register(
          "cached_message_bytes", "Number of locally cached messages in bytes", "1")),
      delivery_latency_(opencensus::stats::MeasureInt64::Register(
          "delivery_latency", "Time spent delivering messages from servers to clients", "1")),
      await_time_(opencensus::stats::MeasureInt64::Register(
          "await_time", "Client side queuing time of messages before getting processed", "1")),
      process_time_(opencensus::stats::MeasureInt64::Register("process_time", "Process message time", "1")) {
  opencensus::stats::ViewDescriptor()
      .set_name("rocketmq_consumer_cached_messages")
      .set_description("Number of messages locally cached")
      .set_measure("cached_message_quantity")
      .set_aggregation(opencensus::stats::Aggregation::LastValue())
      .add_column(Tag::topicTag())
      .add_column(Tag::clientIdTag())
      .RegisterForExport();

  opencensus::stats::ViewDescriptor()
      .set_name("rocketmq_consumer_cached_bytes")
      .set_description("Number of locally cached messages in bytes")
      .set_measure("cached_message_bytes")
      .set_aggregation(opencensus::stats::Aggregation::LastValue())
      .add_column(Tag::topicTag())
      .add_column(Tag::clientIdTag())
      .RegisterForExport();

  opencensus::stats::ViewDescriptor()
      .set_name("rocketmq_delivery_latency")
      .set_description("Message delivery latency")
      .set_measure("delivery_latency")
      .set_aggregation(opencensus::stats::Aggregation::Distribution(
          opencensus::stats::BucketBoundaries::Explicit({1.0, 5.0, 10.0, 20.0, 50.0, 200.0, 500.0})))
      .add_column(Tag::topicTag())
      .add_column(Tag::clientIdTag())
      .RegisterForExport();

  opencensus::stats::ViewDescriptor()
      .set_name("rocketmq_await_time")
      .set_description("Message await time")
      .set_measure("await_time")
      .set_aggregation(opencensus::stats::Aggregation::Distribution(
          opencensus::stats::BucketBoundaries::Explicit({1.0, 5.0, 20.0, 100.0, 1000.0, 5000.0, 10000.0})))
      .add_column(Tag::topicTag())
      .add_column(Tag::clientIdTag())
      .RegisterForExport();

  opencensus::stats::ViewDescriptor()
      .set_name("rocketmq_process_time")
      .set_description("Process time")
      .set_measure("process_time")
      .set_aggregation(opencensus::stats::Aggregation::Distribution(
          opencensus::stats::BucketBoundaries::Explicit({1.0, 5.0, 10.0, 100.0, 10000.0, 60000.0})))
      .add_column(Tag::topicTag())
      .add_column(Tag::clientIdTag())
      .RegisterForExport();
}

ROCKETMQ_NAMESPACE_END