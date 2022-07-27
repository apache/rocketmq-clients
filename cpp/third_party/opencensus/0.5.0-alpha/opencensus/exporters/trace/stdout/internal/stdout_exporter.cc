// Copyright 2018, OpenCensus Authors
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

#include "opencensus/exporters/trace/stdout/stdout_exporter.h"

#include <iostream>
#include <vector>

#include "absl/base/macros.h"
#include "absl/memory/memory.h"
#include "opencensus/trace/exporter/span_data.h"
#include "opencensus/trace/exporter/span_exporter.h"

namespace opencensus {
namespace exporters {
namespace trace {
namespace {

class Handler : public ::opencensus::trace::exporter::SpanExporter::Handler {
 public:
  explicit Handler(std::ostream* stream) : stream_(stream) {}

  void Export(const std::vector<::opencensus::trace::exporter::SpanData>& spans)
      override {
    for (const auto& span : spans) {
      *stream_ << span.DebugString() << "\n";
    }
    stream_->flush();
  }

 private:
  std::ostream* stream_;
};

}  // namespace

// static
void StdoutExporter::Register(std::ostream* stream) {
  ::opencensus::trace::exporter::SpanExporter::RegisterHandler(
      absl::make_unique<Handler>(stream));
}

}  // namespace trace
}  // namespace exporters
}  // namespace opencensus
