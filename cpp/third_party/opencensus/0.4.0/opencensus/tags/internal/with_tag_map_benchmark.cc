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

#include "opencensus/tags/with_tag_map.h"

#include <cstdlib>

#include "benchmark/benchmark.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/tags/tag_map.h"

namespace opencensus {
namespace tags {
namespace {

// Returns an example TagMap.
TagMap Tags() {
  static const auto k1 = TagKey::Register("key1");
  static const auto k2 = TagKey::Register("key2");
  static const auto k3 = TagKey::Register("key3");
  return TagMap({{k1, "val1"}, {k2, "val2"}, {k3, "val3"}});
}

void BM_WithTagMapCopy(benchmark::State& state) {
  const auto tags = Tags();
  for (auto _ : state) {
    WithTagMap wt(tags);
  }
}
BENCHMARK(BM_WithTagMapCopy);

void BM_WithTagMapCopyConditionFalse(benchmark::State& state) {
  const auto tags = Tags();
  for (auto _ : state) {
    WithTagMap wt(tags, false);
  }
}
BENCHMARK(BM_WithTagMapCopyConditionFalse);

void BM_TagMapConstruct(benchmark::State& state) {
  for (auto _ : state) {
    auto tags = Tags();
    benchmark::DoNotOptimize(tags);
  }
}
BENCHMARK(BM_TagMapConstruct);

void BM_WithTagMapConstructAndCopy(benchmark::State& state) {
  for (auto _ : state) {
    const auto tags = Tags();
    WithTagMap wt(tags);
  }
}
BENCHMARK(BM_WithTagMapConstructAndCopy);

void BM_WithTagMapConstructAndMove(benchmark::State& state) {
  for (auto _ : state) {
    auto tags = Tags();
    WithTagMap wt(std::move(tags));
  }
}
BENCHMARK(BM_WithTagMapConstructAndMove);

}  // namespace
}  // namespace tags
}  // namespace opencensus

BENCHMARK_MAIN();
