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

#include "opencensus/trace/propagation/b3.h"

#include <cstdint>
#include <cstring>
#include <string>

#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_id.h"
#include "opencensus/trace/trace_options.h"

namespace opencensus {
namespace trace {
namespace propagation {

namespace {

// Returns true if the string only contains valid hex digits.
bool IsHexDigits(absl::string_view s) {
  for (int i = 0; i < s.length(); ++i) {
    if (!absl::ascii_isxdigit(s[i])) return false;
  }
  return true;
}

}  // namespace

SpanContext FromB3Headers(absl::string_view b3_trace_id,
                          absl::string_view b3_span_id,
                          absl::string_view b3_sampled,
                          absl::string_view b3_flags) {
  static SpanContext invalid;
  uint8_t sampled;

  if (b3_sampled == "1") {
    sampled = 1;
  } else if (b3_sampled == "0" || b3_sampled.empty()) {
    sampled = 0;
  } else {
    return invalid;
  }

  if (b3_flags == "1") {
    sampled = 1;
  } else if (!b3_flags.empty()) {
    return invalid;
  }

  if (b3_trace_id.length() != 32 && b3_trace_id.length() != 16) return invalid;
  if (b3_span_id.length() != 16) return invalid;

  if (!IsHexDigits(b3_trace_id)) {
    return invalid;
  }
  if (!IsHexDigits(b3_span_id)) {
    return invalid;
  }

  std::string trace_id_binary = absl::HexStringToBytes(b3_trace_id);
  std::string span_id_binary = absl::HexStringToBytes(b3_span_id);

  uint8_t extended_trace_id[16];

  // trace_id_ptr must point to a 128-bit trace_id.
  const uint8_t* trace_id_ptr;
  if (trace_id_binary.length() == 16) {
    trace_id_ptr = reinterpret_cast<const uint8_t*>(trace_id_binary.data());
  } else if (trace_id_binary.length() == 8) {
    // Extend 64-bit trace_id to 128-bit using the buffer.
    memset(extended_trace_id, 0, 8);
    memcpy(extended_trace_id + 8, trace_id_binary.data(), 8);
    trace_id_ptr = extended_trace_id;
  } else {
    return invalid;
  }

  return SpanContext(
      TraceId(trace_id_ptr),
      SpanId(reinterpret_cast<const uint8_t*>(span_id_binary.data())),
      TraceOptions(&sampled));
}

std::string ToB3TraceIdHeader(const SpanContext& ctx) {
  return ctx.trace_id().ToHex();
}

std::string ToB3SpanIdHeader(const SpanContext& ctx) {
  return ctx.span_id().ToHex();
}

std::string ToB3SampledHeader(const SpanContext& ctx) {
  return ctx.trace_options().IsSampled() ? "1" : "0";
}

}  // namespace propagation
}  // namespace trace
}  // namespace opencensus
