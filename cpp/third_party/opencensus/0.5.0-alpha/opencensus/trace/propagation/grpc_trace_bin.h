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

#ifndef OPENCENSUS_TRACE_PROPAGATION_GRPC_TRACE_BIN_H_
#define OPENCENSUS_TRACE_PROPAGATION_GRPC_TRACE_BIN_H_

#include <cstdint>
#include <string>

#include "absl/strings/string_view.h"
#include "opencensus/trace/span_context.h"

namespace opencensus {
namespace trace {
namespace propagation {

// Parses the value of the binary grpc-trace-bin header, returning a
// SpanContext. If parsing fails, IsValid will be false.
//
// Example value, hex encoded:
//   00                               (version)
//   00                               (trace_id field)
//   12345678901234567890123456789012 (trace_id)
//   01                               (span_id field)
//   0000000000003039                 (span_id)
//   02                               (trace_options field)
//   01                               (options: enabled)
//
// See also:
// https://github.com/census-instrumentation/opencensus-specs/blob/master/encodings/BinaryEncoding.md
SpanContext FromGrpcTraceBinHeader(absl::string_view header);

// Returns a value for the grpc-trace-bin header.
std::string ToGrpcTraceBinHeader(const SpanContext& ctx);

// The length of the grpc-trace-bin value:
//      1 (version)
//   +  1 (trace_id field)
//   + 16 (length of trace_id)
//   +  1 (span_id field)
//   +  8 (span_id length)
//   +  1 (trace_options field)
//   +  1 (trace_options length)
//   ----
//     29
constexpr int kGrpcTraceBinHeaderLen = 29;

// Fills a pre-allocated buffer with the value for the grpc-trace-bin header.
// The buffer must be at least kGrpcTraceBinHeaderLen bytes long.
void ToGrpcTraceBinHeader(const SpanContext& ctx, uint8_t* out);

}  // namespace propagation
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_PROPAGATION_GRPC_TRACE_BIN_H_
