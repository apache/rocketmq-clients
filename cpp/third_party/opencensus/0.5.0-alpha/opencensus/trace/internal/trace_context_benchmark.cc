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

#include "opencensus/trace/propagation/trace_context.h"

#include "benchmark/benchmark.h"

namespace opencensus {
namespace trace {
namespace propagation {
namespace {

constexpr char kHeader[] =
    "00-404142434445464748494a4b4c4d4e4f-6162636465666768-01";

void BM_FromTraceParentHeader(benchmark::State& state) {
  while (state.KeepRunning()) {
    FromTraceParentHeader(kHeader);
  }
}
BENCHMARK(BM_FromTraceParentHeader);

void BM_ToTraceParentHeader(benchmark::State& state) {
  auto ctx = FromTraceParentHeader(kHeader);
  while (state.KeepRunning()) {
    ToTraceParentHeader(ctx);
  }
}
BENCHMARK(BM_ToTraceParentHeader);

}  // namespace
}  // namespace propagation
}  // namespace trace
}  // namespace opencensus

BENCHMARK_MAIN();
