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
#include <memory>
#include <string>
#include <vector>

#include "Protocol.h"
#include "RetryPolicy.h"
#include "absl/container/flat_hash_map.h"
#include "absl/time/time.h"
#include "rocketmq/CredentialsProvider.h"
#include "rocketmq/Tracing.h"

ROCKETMQ_NAMESPACE_BEGIN

struct PublisherConfig {
  std::vector<rmq::Resource> topics;
  std::uint32_t max_body_size{4194304};
};

struct SubscriberConfig {
  rmq::Resource group;
  absl::flat_hash_map<std::string, rmq::SubscriptionEntry> subscriptions;
  bool fifo{false};
  bool fifo_consume_accelerator{false};
  std::uint32_t receive_batch_size{32};
  absl::Duration polling_timeout{absl::Seconds(30)};
};

struct Metric {
  bool on{false};
  rmq::Endpoints endpoints;
};

struct ClientConfig {
  std::string client_id;
  rmq::ClientType client_type{rmq::ClientType::CLIENT_TYPE_UNSPECIFIED};
  RetryPolicy backoff_policy;
  // TODO: use std::chrono::milliseconds
  absl::Duration request_timeout{absl::Seconds(3)};
  std::string region{"cn-hangzhou"};
  std::string resource_namespace;
  std::shared_ptr<CredentialsProvider> credentials_provider;
  PublisherConfig publisher;
  SubscriberConfig subscriber;
  Metric metric;
  bool withSsl;
  std::unique_ptr<opencensus::trace::Sampler> sampler_;
};

ROCKETMQ_NAMESPACE_END