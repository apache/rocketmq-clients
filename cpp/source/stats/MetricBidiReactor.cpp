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
#include "MetricBidiReactor.h"

#include <chrono>

#include "OpencensusExporter.h"
#include "Signature.h"
#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

MetricBidiReactor::MetricBidiReactor(std::shared_ptr<Client> client, std::shared_ptr<OpencensusExporter> exporter)
    : client_(client), exporter_(exporter) {
  Metadata metadata;
  Signature::sign(client->config(), metadata);

  for (const auto& entry : metadata) {
    context_.AddMetadata(entry.first, entry.second);
  }
  context_.set_deadline(std::chrono::system_clock::now() + std::chrono::hours(24));

  auto exporter_ptr = exporter_.lock();
  if (!exporter_ptr) {
    SPDLOG_WARN("Exporter has already been destructed");
    return;
  }
  exporter_ptr->stub()->async()->Export(&context_, this);
  AddHold();
  StartCall();
}

void MetricBidiReactor::OnReadDone(bool ok) {
  if (!ok) {
    SPDLOG_WARN("Failed to read response");
    // match the AddHold() call in MetricBidiReactor::fireRead
    RemoveHold();
    return;
  }
  SPDLOG_DEBUG("OnReadDone OK");
  StartRead(&response_);
}

void MetricBidiReactor::OnWriteDone(bool ok) {
  {
    bool expected = true;
    if (!inflight_.compare_exchange_strong(expected, false, std::memory_order_relaxed)) {
      SPDLOG_WARN("Illegal command-inflight state");
      return;
    }
  }

  if (!ok) {
    SPDLOG_WARN("Failed to report metrics");
    // match AddHold() call in MetricBidiReactor::MetricBidiReactor
    RemoveHold();
    return;
  }
  SPDLOG_DEBUG("OnWriteDone OK");

  // If the read stream has not started yet, start it now.
  fireRead();

  // Remove the one that been written.
  {
    absl::MutexLock lk(&requests_mtx_);
    requests_.pop_front();
  }

  tryWriteNext();
}

void MetricBidiReactor::OnDone(const grpc::Status& s) {
  auto client = client_.lock();
  if (!client) {
    return;
  }

  if (s.ok()) {
    SPDLOG_DEBUG("Bi-directional stream ended. status.code={}, status.message={}", s.error_code(), s.error_message());
  } else {
    SPDLOG_WARN("Bi-directional stream ended. status.code={}, status.message={}", s.error_code(), s.error_message());
    auto exporter = exporter_.lock();
    if (exporter) {
      exporter->resetStream();
    }
  }
}

void MetricBidiReactor::write(ExportMetricsServiceRequest request) {
  SPDLOG_DEBUG("Append ExportMetricsServiceRequest to buffer");
  {
    absl::MutexLock lk(&requests_mtx_);
    requests_.push_back(std::move(request));
  }

  tryWriteNext();
}

void MetricBidiReactor::tryWriteNext() {
  {
    absl::MutexLock lk(&requests_mtx_);
    if (requests_.empty()) {
      SPDLOG_DEBUG("No more metric data to write");
      return;
    }
  }

  bool expected = false;
  if (inflight_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
    absl::MutexLock lk(&requests_mtx_);
    SPDLOG_DEBUG("MetricBidiReactor#StartWrite");
    StartWrite(&(requests_.front()));
  }
}

void MetricBidiReactor::fireRead() {
  bool expected = false;
  if (read_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
    AddHold();
    StartRead(&response_);
  }
}

ROCKETMQ_NAMESPACE_END