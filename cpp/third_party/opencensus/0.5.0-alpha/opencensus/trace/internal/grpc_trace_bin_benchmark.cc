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

#include "opencensus/trace/propagation/grpc_trace_bin.h"

#include "benchmark/benchmark.h"

namespace opencensus {
namespace trace {
namespace propagation {
namespace {

constexpr unsigned char header_data[] = {
    0,                                               // version_id
    0,                                               // trace_id field
    0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x70, 0x71,  // lo
    0x72, 0x73, 0x74, 0x75, 0x76, 0x77, 0x78, 0x79,  // hi
    1,                                               // span_id field
    0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88,  // span_id
    2,                                               // trace_options field
    1,                                               // tracing enabled
};

void BM_FromGrpcTraceBin(benchmark::State& state) {
  absl::string_view header(reinterpret_cast<const char*>(header_data),
                           sizeof(header_data));
  while (state.KeepRunning()) {
    FromGrpcTraceBinHeader(header);
  }
}
BENCHMARK(BM_FromGrpcTraceBin);

void BM_FromGrpcTraceBin_Invalid(benchmark::State& state) {
  while (state.KeepRunning()) {
    FromGrpcTraceBinHeader("");
  }
}
BENCHMARK(BM_FromGrpcTraceBin_Invalid);

void BM_ToGrpcTraceBin(benchmark::State& state) {
  absl::string_view header(reinterpret_cast<const char*>(header_data),
                           sizeof(header_data));
  auto ctx = FromGrpcTraceBinHeader(header);
  while (state.KeepRunning()) {
    ToGrpcTraceBinHeader(ctx);
  }
}
BENCHMARK(BM_ToGrpcTraceBin);

void BM_ToGrpcTraceBin_InPlace(benchmark::State& state) {
  absl::string_view header(reinterpret_cast<const char*>(header_data),
                           sizeof(header_data));
  auto ctx = FromGrpcTraceBinHeader(header);
  uint8_t out[kGrpcTraceBinHeaderLen];
  while (state.KeepRunning()) {
    ToGrpcTraceBinHeader(ctx, out);
  }
}
BENCHMARK(BM_ToGrpcTraceBin_InPlace);

}  // namespace
}  // namespace propagation
}  // namespace trace
}  // namespace opencensus

BENCHMARK_MAIN();
