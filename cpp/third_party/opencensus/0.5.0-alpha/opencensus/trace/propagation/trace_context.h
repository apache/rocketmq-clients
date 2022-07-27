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

#ifndef OPENCENSUS_TRACE_PROPAGATION_TRACE_CONTEXT_H_
#define OPENCENSUS_TRACE_PROPAGATION_TRACE_CONTEXT_H_

#include <string>

#include "absl/strings/string_view.h"
#include "opencensus/trace/span_context.h"

namespace opencensus {
namespace trace {
namespace propagation {

// Implementation of the TraceContext propagation protocol:
// https://github.com/w3c/distributed-tracing

// Parses the value of the "traceparent: ..." header, returning a
// SpanContext. If parsing fails, IsValid will be false.
//
// The input format is a lowercase hex string:
//   - version_id: 1 byte, currently must be zero - hex encoded (2 characters)
//   - trace_id: 16 bytes (opaque blob) - hex encoded (32 characters)
//   - span_id: 8 bytes (opaque blob) - hex encoded (16 characters)
//   - trace_options: 1 byte (LSB means tracing enabled) - hex encoded (2 ch)
//
// Example: "00-404142434445464748494a4b4c4d4e4f-6162636465666768-01"
//           v  trace_id                         span_id          options
//
SpanContext FromTraceParentHeader(absl::string_view header);

// Returns a value for the traceparent header.
std::string ToTraceParentHeader(const SpanContext& ctx);

}  // namespace propagation
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_PROPAGATION_TRACE_CONTEXT_H_
