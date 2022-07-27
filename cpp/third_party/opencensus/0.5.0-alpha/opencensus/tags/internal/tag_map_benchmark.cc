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

#include "opencensus/tags/tag_map.h"

#include <utility>
#include <vector>

#include "absl/strings/str_cat.h"
#include "benchmark/benchmark.h"
#include "opencensus/tags/tag_key.h"

namespace opencensus {
namespace tags {
namespace {

void BM_MakeTagMap(benchmark::State& state) {
  // Create a vector with N tags.
  const int n = state.range(0);
  std::vector<std::pair<TagKey, std::string>> tags;
  tags.reserve(n);
  for (int i = 0; i < n; ++i) {
    tags.emplace_back(TagKey::Register(absl::StrCat("key", i)),
                      absl::StrCat("val", i));
  }
  // Benchmark TagMap initialization.
  for (auto _ : state) {
    TagMap tm(tags);
  }
}
BENCHMARK(BM_MakeTagMap)->RangeMultiplier(2)->Range(1, 32);

}  // namespace
}  // namespace tags
}  // namespace opencensus

BENCHMARK_MAIN();
