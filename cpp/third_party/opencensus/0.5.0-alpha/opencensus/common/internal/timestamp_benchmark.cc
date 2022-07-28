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

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "benchmark/benchmark.h"
#include "google/protobuf/timestamp.pb.h"
#include "opencensus/common/internal/timestamp.h"

namespace {

void BM_SetTimestamp(benchmark::State& state) {
  absl::Time t = absl::Now();
  google::protobuf::Timestamp proto;
  for (auto _ : state) {
    ::opencensus::common::SetTimestamp(t, &proto);
  }
}
BENCHMARK(BM_SetTimestamp);

}  // namespace

BENCHMARK_MAIN();
