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

#include "absl/strings/string_view.h"
#include "benchmark/benchmark.h"
#include "opencensus/tags/propagation/grpc_tags_bin.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/tags/tag_map.h"

namespace opencensus {
namespace tags {
namespace propagation {
namespace {

void BM_FromGrpcTagsBinHeader(benchmark::State& state) {
  constexpr char tagsbin[] = {
      0,                 // Version
      0,                 // Tag field
      3, 'k', 'e', 'y',  // k1
      3, 'v', 'a', 'l',  // v1
  };
  const absl::string_view hdr(tagsbin, sizeof(tagsbin));
  TagMap m({});
  for (auto _ : state) {
    FromGrpcTagsBinHeader(hdr, &m);
  }
}
BENCHMARK(BM_FromGrpcTagsBinHeader);

void BM_ToGrpcTagsBinHeader(benchmark::State& state) {
  TagMap m({{TagKey::Register("key"), "val"}});
  for (auto _ : state) {
    ToGrpcTagsBinHeader(m);
  }
}
BENCHMARK(BM_ToGrpcTagsBinHeader);

}  // namespace
}  // namespace propagation
}  // namespace tags
}  // namespace opencensus

BENCHMARK_MAIN();
