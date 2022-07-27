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

#ifndef OPENCENSUS_TRACE_EXPORTER_SPAN_EXPORTER_H_
#define OPENCENSUS_TRACE_EXPORTER_SPAN_EXPORTER_H_

#include <memory>
#include <vector>

#include "absl/time/time.h"
#include "opencensus/trace/exporter/span_data.h"

namespace opencensus {
namespace trace {
namespace exporter {

// SpanExporter allows Exporters to register. Thread-safe.
class SpanExporter final {
 public:
  // Sets the batch size when exporting traces. Takes effect after the next
  // batch starts. This is not a strict limit, the generated batch may be
  // slightly larger. If the interval expires before the batch fills up, the
  // batch will be smaller.
  //
  // Warning: this API may be removed in future, in favor of configuring this
  // per-exporter.
  static void SetBatchSize(int size);

  // Sets the interval between exporting batches of traces. Takes effect after
  // the next batch starts.
  //
  // Warning: this API may be removed in future, in favor of configuring this
  // per-exporter.
  static void SetInterval(absl::Duration interval);

  // Handlers allow different tracing services to export recorded data for
  // sampled spans in their own format. Every exporter must provide a static
  // Register() method that takes any arguments needed by the exporter (e.g. a
  // URL to export to) and calls SpanExporter::RegisterHandler itself.
  class Handler {
   public:
    virtual ~Handler() = default;
    virtual void Export(const std::vector<SpanData>& spans) = 0;
  };

  // This should only be called by Handler's Register() method.
  static void RegisterHandler(std::unique_ptr<Handler> handler);

 private:
  SpanExporter() = delete;
  friend class SpanExporterTestPeer;

  // Forces an export, only for testing purposes.
  static void ExportForTesting();
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_EXPORTER_SPAN_EXPORTER_H_
