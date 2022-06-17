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

#include "LoggerImpl.h"
#include "OpencensusExporter.h"
#include "Signature.h"

ROCKETMQ_NAMESPACE_BEGIN

MetricBidiReactor::MetricBidiReactor(std::weak_ptr<Client> client, std::weak_ptr<OpencensusExporter> exporter)
    : client_(client), exporter_(exporter) {
  grpc::ClientContext context;
  auto ptr = client_.lock();

  Metadata metadata;
  Signature::sign(ptr->config(), metadata);

  for (const auto& entry : metadata) {
    context.AddMetadata(entry.first, entry.second);
  }
  context.set_deadline(std::chrono::system_clock::now() + absl::ToChronoMilliseconds(ptr->config().request_timeout));

  auto exporter_ptr = exporter_.lock();
  if (!exporter_ptr) {
    return;
  }

  exporter_ptr->stub()->async()->Export(&context, this);
  StartCall();
}

void MetricBidiReactor::OnReadDone(bool ok) {
  if (!ok) {
    SPDLOG_WARN("Failed to read response");
    return;
  }

  StartRead(&response_);
}

void MetricBidiReactor::OnWriteDone(bool ok) {
  if (!ok) {
    SPDLOG_WARN("Failed to report metrics");
    return;
  }

  bool expected = true;
  if (inflight_.compare_exchange_strong(expected, false, std::memory_order_relaxed)) {
    fireWrite();
  }
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
  }
}

void MetricBidiReactor::write(ExportMetricsServiceRequest request) {
  {
    absl::MutexLock lk(&requests_mtx_);
    requests_.emplace_back(std::move(request));
  }

  fireWrite();
}

void MetricBidiReactor::fireWrite() {
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
    request_.CopyFrom(requests_[0]);
    requests_.erase(requests_.begin());
    StartWrite(&request_);
    fireRead();
  }
}

void MetricBidiReactor::fireRead() {
  bool expected = false;
  if (read_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
    StartRead(&response_);
  }
}

ROCKETMQ_NAMESPACE_END