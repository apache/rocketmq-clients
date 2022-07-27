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

#ifndef OPENCENSUS_TRACE_PROPAGATION_CLOUD_TRACE_CONTEXT_H_
#define OPENCENSUS_TRACE_PROPAGATION_CLOUD_TRACE_CONTEXT_H_

#include <string>

#include "absl/strings/string_view.h"
#include "opencensus/trace/span_context.h"

namespace opencensus {
namespace trace {
namespace propagation {

// Parses the value of the "X-Cloud-Trace-Context: ..." header, returning a
// SpanContext. If parsing fails, IsValid will be false.
//
// The input format is: trace-id/span-id[;o=options]
// Where:
//   - trace-id is an opaque 16-byte binary string, encoded as 32 hex digits.
//   It must not be all zeroes.
//
//   - span-id is a decimal representation of a big-endian 64 bit value, and
//   must not have a value of zero.
//
//   - options is a single char from '0' to '3', but only 1 and 3 enable
//   tracing.
//
// Example: "12345678901234567890123456789012/12345;o=1"
//
// See also: https://cloud.google.com/trace/docs/troubleshooting
SpanContext FromCloudTraceContextHeader(absl::string_view header);

// Returns a value for the X-Cloud-Trace-Context header.
std::string ToCloudTraceContextHeader(const SpanContext& ctx);

}  // namespace propagation
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_PROPAGATION_CLOUD_TRACE_CONTEXT_H_
