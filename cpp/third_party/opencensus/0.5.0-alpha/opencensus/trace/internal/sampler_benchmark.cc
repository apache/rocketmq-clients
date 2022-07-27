// Copyright 2019, OpenCensus Authors
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

#include <string>
#include <vector>

#include "benchmark/benchmark.h"
#include "opencensus/trace/sampler.h"
#include "opencensus/trace/span.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_id.h"

namespace opencensus {
namespace trace {
namespace {

void BM_ProbabilitySampler(benchmark::State& state) {
  // Unused:
  SpanContext parent_context;
  bool has_remote_parent = true;
  SpanId span_id;
  std::string name = "MyName";
  std::vector<Span*> parent_links;
  // Used:
  constexpr uint8_t trace_id_buf[TraceId::kSize] = {
      1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};
  TraceId trace_id(trace_id_buf);
  static ProbabilitySampler sampler(.5);

  while (state.KeepRunning()) {
    sampler.ShouldSample(&parent_context, has_remote_parent, trace_id, span_id,
                         name, parent_links);
  }
}
BENCHMARK(BM_ProbabilitySampler);

}  // namespace
}  // namespace trace
}  // namespace opencensus

BENCHMARK_MAIN();
