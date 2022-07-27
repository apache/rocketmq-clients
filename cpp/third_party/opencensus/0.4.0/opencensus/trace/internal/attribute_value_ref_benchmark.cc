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

#include "benchmark/benchmark.h"
#include "opencensus/trace/attribute_value_ref.h"

namespace opencensus {
namespace trace {
namespace {

void BM_ConstructFromLiteral(benchmark::State& state) {
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(
        AttributeValueRef("literal string, showing no string copy happens"));
  }
}
BENCHMARK(BM_ConstructFromLiteral);

void BM_ConstructFromString(benchmark::State& state) {
  std::string s(8192, 'a');
  while (state.KeepRunning()) {
    benchmark::DoNotOptimize(AttributeValueRef(s));
  }
}
BENCHMARK(BM_ConstructFromString);

}  // namespace
}  // namespace trace
}  // namespace opencensus

BENCHMARK_MAIN();
