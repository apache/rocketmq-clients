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

#ifndef OPENCENSUS_TRACE_PROPAGATION_B3_H_
#define OPENCENSUS_TRACE_PROPAGATION_B3_H_

#include <string>

#include "absl/strings/string_view.h"
#include "opencensus/trace/span_context.h"

namespace opencensus {
namespace trace {
namespace propagation {

// Implementation of the B3 propagation format:
// https://github.com/openzipkin/b3-propagation

// We handle:
//   X-B3-TraceId
//   X-B3-SpanId
//   X-B3-Sampled
//   X-B3-Flags
//
// But not:
//   X-B3-ParentSpanId
//   b3

// Parses the values of the given X-B3 headers, returning a SpanContext. If
// parsing fails, IsValid will be false.
//
// b3_trace_id: 32 or 16 lowercase hex chars.
// b3_span_id: 16 lowercase hex chars.
// b3_sampled: "1" or "0" or empty string.
// b3_flags: "1" or empty string.
//
// The X-B3-ParentSpanId header is not used.
//
SpanContext FromB3Headers(absl::string_view b3_trace_id,
                          absl::string_view b3_span_id,
                          absl::string_view b3_sampled,
                          absl::string_view b3_flags);

// Returns a value for the X-B3-TraceId header.
std::string ToB3TraceIdHeader(const SpanContext& ctx);

// Returns a value for the X-B3-SpanId header.
std::string ToB3SpanIdHeader(const SpanContext& ctx);

// Returns a value for the X-B3-Sampled header.
std::string ToB3SampledHeader(const SpanContext& ctx);

}  // namespace propagation
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_PROPAGATION_B3_H_
