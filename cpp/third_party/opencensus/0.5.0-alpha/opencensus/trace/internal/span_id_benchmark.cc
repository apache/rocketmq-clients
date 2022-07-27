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

#include "benchmark/benchmark.h"
#include "opencensus/trace/span_id.h"

namespace opencensus {
namespace trace {
namespace {

constexpr uint8_t span_id[] = {1, 2, 3, 4, 5, 6, 7, 8};

void BM_SpanIdDefaultConstructor(benchmark::State& state) {
  while (state.KeepRunning()) {
    SpanId id;
  }
}
BENCHMARK(BM_SpanIdDefaultConstructor);

void BM_SpanIdConstructFromBuffer(benchmark::State& state) {
  while (state.KeepRunning()) {
    SpanId id(span_id);
  }
}
BENCHMARK(BM_SpanIdConstructFromBuffer);

void BM_SpanIdToHex(benchmark::State& state) {
  SpanId id(span_id);
  while (state.KeepRunning()) {
    id.ToHex();
  }
}
BENCHMARK(BM_SpanIdToHex);

void BM_SpanIdCompareEqual(benchmark::State& state) {
  bool b;
  SpanId id1(span_id);
  SpanId id2(span_id);
  while (state.KeepRunning()) {
    b = id1 == id2;
    (void)b;
  }
}
BENCHMARK(BM_SpanIdCompareEqual);

void BM_SpanIdIsValidFalse(benchmark::State& state) {
  SpanId id;
  while (state.KeepRunning()) {
    id.IsValid();
  }
}
BENCHMARK(BM_SpanIdIsValidFalse);

void BM_SpanIdIsValidTrue(benchmark::State& state) {
  SpanId id(span_id);
  while (state.KeepRunning()) {
    id.IsValid();
  }
}
BENCHMARK(BM_SpanIdIsValidTrue);

void BM_SpanIdCopyTo(benchmark::State& state) {
  uint8_t buf[SpanId::kSize];
  SpanId id(span_id);
  while (state.KeepRunning()) {
    id.CopyTo(buf);
  }
}
BENCHMARK(BM_SpanIdCopyTo);

}  // namespace
}  // namespace trace
}  // namespace opencensus
BENCHMARK_MAIN();
