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

#ifndef OPENCENSUS_TRACE_EXPORTER_LOCAL_SPAN_STORE_H_
#define OPENCENSUS_TRACE_EXPORTER_LOCAL_SPAN_STORE_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "opencensus/trace/exporter/span_data.h"
#include "opencensus/trace/status_code.h"

namespace opencensus {
namespace trace {
namespace exporter {

// **WARNING** This code is subject to change. Do not rely on its API or
// implementation functioning in the current manner.
//
//
// LocalSpanStore allows users to access in-process information about Spans that
// have completed (called End()) and were recording events.
//
// The LocalSpanStore has a bounded size and evicts Spans when needed.
//
// This class is thread-safe.
class LocalSpanStore {
 public:
  // Samples based on latency for successful spans are collected in these
  // buckets. Ranges are half-open, e.g. [0..10us).
  enum LatencyBucketBoundary {
    k0_to_10us,
    k10us_to_100us,
    k100us_to_1ms,
    k1ms_to_10ms,
    k10ms_to_100ms,
    k100ms_to_1s,
    k1s_to_10s,
    k10s_to_100s,
    k100s_plus,
  };

  struct PerSpanNameSummary {
    std::unordered_map<LatencyBucketBoundary, int, std::hash<int>>
        number_of_latency_sampled_spans;
    std::unordered_map<StatusCode, int, std::hash<int>>
        number_of_error_sampled_spans;
  };

  struct Summary {
    std::unordered_map<std::string, PerSpanNameSummary> per_span_name_summary;
  };

  // Filters all the spans based on exact match of span name and latency in the
  // half-open interval [lower_latency_ns, upper_latency_ns). If span_name is
  // blank, doesn't filter by name. Returns at most max_spans_to_return spans.
  struct LatencyFilter {
    std::string span_name;
    int max_spans_to_return;
    uint64_t lower_latency_ns;
    uint64_t upper_latency_ns;
  };

  // Filters all the spans based on exact match of span name and canonical code.
  // If span_name is blank, doesn't filter by name. If all_errors is true, then
  // ignores canonical code and returns all errors. Returns at most
  // max_spans_to_return spans.
  struct ErrorFilter {
    std::string span_name;
    int max_spans_to_return;
    StatusCode canonical_code;
    bool all_errors;
  };

  // --- Methods ---

  LocalSpanStore() = delete;

  // Returns a summary of the data available in the LocalSpanStore.
  static Summary GetSummary();

  // Returns SpanData for the sampled spans that match the latency filter.
  static std::vector<SpanData> GetLatencySampledSpans(
      const LatencyFilter& filter);

  // Returns SpanData for the sampled spans that match the error filter.
  static std::vector<SpanData> GetErrorSampledSpans(const ErrorFilter& filter);

  // Returns SpanData for all spans in the local span store.
  static std::vector<SpanData> GetSpans();
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_EXPORTER_LOCAL_SPAN_STORE_H_
