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

#include "benchmark/benchmark.h"
#include "opencensus/trace/propagation/b3.h"

namespace opencensus {
namespace trace {
namespace propagation {
namespace {

void BM_FromB3Headers_128bitTraceId(benchmark::State& state) {
  while (state.KeepRunning()) {
    FromB3Headers("463ac35c9f6413ad48485a3953bb6124", "0020000000000001", "1",
                  "");
  }
}
BENCHMARK(BM_FromB3Headers_128bitTraceId);

void BM_FromB3Headers_64bitTraceId(benchmark::State& state) {
  while (state.KeepRunning()) {
    FromB3Headers("1234567812345678", "0020000000000001", "1", "");
  }
}
BENCHMARK(BM_FromB3Headers_64bitTraceId);

void BM_FromB3Headers_InvalidTraceId(benchmark::State& state) {
  while (state.KeepRunning()) {
    FromB3Headers("463ac35c9f6413ad48485a3953bb612X", "0020000000000001", "1",
                  "");
  }
}
BENCHMARK(BM_FromB3Headers_InvalidTraceId);

}  // namespace
}  // namespace propagation
}  // namespace trace
}  // namespace opencensus

BENCHMARK_MAIN();
