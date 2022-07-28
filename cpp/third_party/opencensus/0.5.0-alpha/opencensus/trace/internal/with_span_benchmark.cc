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

#include "opencensus/trace/with_span.h"

#include <cstdlib>

#include "benchmark/benchmark.h"
#include "opencensus/trace/sampler.h"
#include "opencensus/trace/span.h"

namespace opencensus {
namespace trace {
namespace {

void BM_WithSpanUnsampled(benchmark::State& state) {
  static ::opencensus::trace::NeverSampler sampler;
  auto span = Span::StartSpan("MySpan", /*parent=*/nullptr, {&sampler});
  if (span.IsRecording()) {
    abort();
  }
  for (auto _ : state) {
    WithSpan ws(span);
  }
  span.End();
}
BENCHMARK(BM_WithSpanUnsampled);

void BM_WithSpanSampled(benchmark::State& state) {
  static ::opencensus::trace::AlwaysSampler sampler;
  auto span = Span::StartSpan("MySpan", /*parent=*/nullptr, {&sampler});
  if (!span.IsRecording()) {
    abort();
  }
  for (auto _ : state) {
    WithSpan ws(span);
  }
  span.End();
}
BENCHMARK(BM_WithSpanSampled);

void BM_WithSpanConditionFalse(benchmark::State& state) {
  auto span = Span::StartSpan("MySpan");
  for (auto _ : state) {
    WithSpan ws(span, false);
  }
  span.End();
}
BENCHMARK(BM_WithSpanConditionFalse);

}  // namespace
}  // namespace trace
}  // namespace opencensus

BENCHMARK_MAIN();
