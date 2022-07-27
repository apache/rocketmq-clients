// Copyright 2017, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "opencensus/trace/internal/span_exporter_impl.h"

#include <utility>

#include "absl/synchronization/mutex.h"
#include "opencensus/trace/exporter/span_data.h"
#include "opencensus/trace/exporter/span_exporter.h"

namespace opencensus {
namespace trace {
namespace exporter {

SpanExporterImpl* SpanExporterImpl::span_exporter_ = nullptr;

SpanExporterImpl* SpanExporterImpl::Get() {
  static SpanExporterImpl* global_span_exporter_impl = new SpanExporterImpl(
      kDefaultBufferSize, absl::Milliseconds(kIntervalWaitTimeInMillis));
  return global_span_exporter_impl;
}

SpanExporterImpl::SpanExporterImpl(uint32_t buffer_size,
                                   absl::Duration interval)
    : buffer_size_(buffer_size), interval_(interval) {}

void SpanExporterImpl::RegisterHandler(
    std::unique_ptr<SpanExporter::Handler> handler) {
  absl::MutexLock l(&handler_mu_);
  handlers_.emplace_back(std::move(handler));
  if (!thread_started_) {
    StartExportThread();
  }
}

void SpanExporterImpl::AddSpan(
    const std::shared_ptr<opencensus::trace::SpanImpl>& span_impl) {
  absl::MutexLock l(&span_mu_);
  if (!collect_spans_) return;
  spans_.emplace_back(span_impl);
}

void SpanExporterImpl::StartExportThread() {
  t_ = std::thread(&SpanExporterImpl::RunWorkerLoop, this);
  thread_started_ = true;
  absl::MutexLock l(&span_mu_);
  collect_spans_ = true;
}

bool SpanExporterImpl::IsBufferFull() const {
  span_mu_.AssertHeld();
  return spans_.size() >= buffer_size_;
}

void SpanExporterImpl::RunWorkerLoop() {
  std::vector<opencensus::trace::exporter::SpanData> span_data_;
  std::vector<std::shared_ptr<opencensus::trace::SpanImpl>> batch_;
  // Thread loops forever.
  // TODO: Add in shutdown mechanism.
  absl::Time next_forced_export_time = absl::Now() + interval_;
  while (true) {
    {
      absl::MutexLock l(&span_mu_);
      // Wait until batch is full or interval time has been exceeded.
      span_mu_.AwaitWithDeadline(
          absl::Condition(this, &SpanExporterImpl::IsBufferFull),
          next_forced_export_time);
      next_forced_export_time = absl::Now() + interval_;
      if (spans_.empty()) {
        continue;
      }
      std::swap(batch_, spans_);
    }
    for (const auto& span : batch_) {
      span_data_.emplace_back(span->ToSpanData());
    }
    batch_.clear();
    Export(span_data_);
    span_data_.clear();
  }
}

void SpanExporterImpl::Export(const std::vector<SpanData>& span_data) {
  // Call each registered handler.
  absl::MutexLock lock(&handler_mu_);
  for (const auto& handler : handlers_) {
    handler->Export(span_data);
  }
}

void SpanExporterImpl::ExportForTesting() {
  std::vector<opencensus::trace::exporter::SpanData> span_data_;
  std::vector<std::shared_ptr<opencensus::trace::SpanImpl>> batch_;
  {
    absl::MutexLock l(&span_mu_);
    std::swap(batch_, spans_);
  }
  span_data_.reserve(batch_.size());
  for (const auto& span : batch_) {
    span_data_.emplace_back(span->ToSpanData());
  }
  Export(span_data_);
}

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
