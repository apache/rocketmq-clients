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

#include "opencensus/trace/exporter/span_exporter.h"

#include <memory>
#include <utility>

#include "absl/time/time.h"
#include "opencensus/trace/internal/span_exporter_impl.h"

namespace opencensus {
namespace trace {
namespace exporter {

// static
void SpanExporter::SetBatchSize(int size) {
  SpanExporterImpl::Get()->SetBatchSize(size);
}

// static
void SpanExporter::SetInterval(absl::Duration interval) {
  SpanExporterImpl::Get()->SetInterval(interval);
}

// static
void SpanExporter::RegisterHandler(std::unique_ptr<Handler> handler) {
  SpanExporterImpl::Get()->RegisterHandler(std::move(handler));
}

// static
void SpanExporter::ExportForTesting() {
  SpanExporterImpl::Get()->ExportForTesting();
}

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
