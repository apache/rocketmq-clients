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

#ifndef OPENCENSUS_TRACE_INTERNAL_SPAN_EXPORTER_IMPL_H_
#define OPENCENSUS_TRACE_INTERNAL_SPAN_EXPORTER_IMPL_H_

#include <functional>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "opencensus/trace/exporter/span_data.h"
#include "opencensus/trace/exporter/span_exporter.h"
#include "opencensus/trace/internal/span_impl.h"

namespace opencensus {
namespace trace {

class Span;

namespace exporter {

// SpanExporterImpl implements the SpanExporter API. Please refer to
// opencensus/trace/exporter/span_exporter.h for usage.
//
// This class is thread-safe and a singleton.
class SpanExporterImpl {
 public:
  // Returns the global instance of SpanExporterImpl.
  static SpanExporterImpl* Get();

  // A shared_ptr to the span is added to a list. The actual conversion to
  // SpanData will take place at a later time via the background thread. This
  // is intended to be called at the Span::End().
  void AddSpan(const std::shared_ptr<opencensus::trace::SpanImpl>& span_impl);

  // Registers a handler with the exporter. This is intended to be done at
  // initialization.
  void RegisterHandler(std::unique_ptr<SpanExporter::Handler> handler);

  static constexpr uint32_t kDefaultBufferSize = 64;
  static constexpr uint32_t kIntervalWaitTimeInMillis = 5000;

 private:
  SpanExporterImpl(uint32_t buffer_size, absl::Duration interval);
  SpanExporterImpl(const SpanExporterImpl&) = delete;
  SpanExporterImpl(SpanExporterImpl&&) = delete;
  SpanExporterImpl& operator=(const SpanExporterImpl&) = delete;
  SpanExporterImpl& operator=(SpanExporterImpl&&) = delete;
  friend class Span;
  friend class SpanExporter;  // For ExportForTesting() only.

  void StartExportThread() EXCLUSIVE_LOCKS_REQUIRED(handler_mu_);
  void RunWorkerLoop();

  // Calls all registered handlers and exports the spans contained in span_data.
  void Export(const std::vector<SpanData>& span_data);

  // Only for testing purposes: runs the export on the current thread and
  // returns when complete.
  void ExportForTesting();

  // Returns true if the spans_ buffer has filled up.
  bool IsBufferFull() const;

  static SpanExporterImpl* span_exporter_;
  const uint32_t buffer_size_;
  const absl::Duration interval_;
  mutable absl::Mutex span_mu_;
  mutable absl::Mutex handler_mu_;
  std::vector<std::shared_ptr<opencensus::trace::SpanImpl>> spans_
      GUARDED_BY(span_mu_);
  std::vector<std::unique_ptr<SpanExporter::Handler>> handlers_
      GUARDED_BY(handler_mu_);
  bool thread_started_ GUARDED_BY(handler_mu_) = false;
  // Don't collect spans until an exporter has been registered.
  bool collect_spans_ GUARDED_BY(span_mu_) = false;
  std::thread t_ GUARDED_BY(handler_mu_);
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_INTERNAL_SPAN_EXPORTER_IMPL_H_
