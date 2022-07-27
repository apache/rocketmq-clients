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

#include <cstddef>
#include <cstdint>
#include <vector>

#include "benchmark/benchmark.h"
#include "opencensus/common/internal/random.h"

namespace {

void BM_GetRandom(benchmark::State& state) {
  for (auto _ : state) {
    ::opencensus::common::Random::GetRandom();
  }
}
BENCHMARK(BM_GetRandom);

void BM_Random64(benchmark::State& state) {
  for (auto _ : state) {
    ::opencensus::common::Random::GetRandom()->GenerateRandom64();
  }
}
BENCHMARK(BM_Random64);

void BM_RandomBuffer(benchmark::State& state) {
  const size_t size = state.range(0);
  std::vector<uint8_t> buffer(size);
  for (auto _ : state) {
    ::opencensus::common::Random::GetRandom()->GenerateRandomBuffer(
        buffer.data(), size);
  }
}
BENCHMARK(BM_RandomBuffer)->Range(1, 16);

}  // namespace
BENCHMARK_MAIN();
