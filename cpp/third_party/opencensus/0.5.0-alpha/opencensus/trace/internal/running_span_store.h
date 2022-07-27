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

#ifndef OPENCENSUS_TRACE_EXPORTER_RUNNING_SPAN_STORE_H_
#define OPENCENSUS_TRACE_EXPORTER_RUNNING_SPAN_STORE_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "opencensus/trace/exporter/span_data.h"

namespace opencensus {
namespace trace {
namespace exporter {

// **WARNING** This code is subject to change. Do not rely on its API or
// implementation functioning in the current manner.
//
//
// RunningSpanStore allows users to access in-process information about all
// running spans. This functionality allows users to debug stuck operations or
// long-lived operations.
//
// Running spans are spans that haven't called End() and are recording events.
//
// This class is thread-safe.
class RunningSpanStore {
 public:
  struct PerSpanNameSummary {
    int num_running_spans;
  };

  struct Summary {
    std::unordered_map<std::string, PerSpanNameSummary> per_span_name_summary;
  };

  // Filters all the spans based on exact match of span name. If span_name is
  // blank, doesn't filter by name. Returns at most max_spans_to_return
  // spans.
  struct Filter {
    std::string span_name;
    int max_spans_to_return;
  };

  // --- Methods ---

  RunningSpanStore() = delete;

  // Returns a summary of the data available in the RunningSpanStore.
  static Summary GetSummary();

  // Returns SpanData for the running spans that match the filter.
  static std::vector<SpanData> GetRunningSpans(const Filter& filter);
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_EXPORTER_RUNNING_SPAN_STORE_H_
