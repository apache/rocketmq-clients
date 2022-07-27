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

#include "opencensus/trace/propagation/cloud_trace_context.h"

#include <cstdint>

#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_id.h"
#include "opencensus/trace/trace_options.h"

#include "absl/base/internal/endian.h"
#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"

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

// Returns a SpanId which is a big-endian encoding of a decimal number.
SpanId FromDecimal(uint64_t n) {
  uint8_t buf[8];
  absl::big_endian::Store64(buf, n);
  return SpanId(buf);
}

// Returns the decimal representation of the SpanId.
uint64_t ToDecimal(const SpanId& span_id) {
  uint64_t n;
  span_id.CopyTo(reinterpret_cast<uint8_t*>(&n));
  return absl::big_endian::ToHost64(n);
}

}  // namespace

SpanContext FromCloudTraceContextHeader(absl::string_view header) {
  constexpr int kTraceIdLen = 16;
  constexpr int kTraceIdLenHex = 2 * kTraceIdLen;
  constexpr int kOptionsLen = 4;  // e.g. ";o=1"
  static SpanContext invalid;

  if (header.size() < kTraceIdLenHex + 2 || header[kTraceIdLenHex] != '/') {
    // Too short to contain a valid trace_id/span_id, or missing slash.
    return invalid;
  }

  // Check if options are present.
  absl::string_view options = header.substr(header.size() - kOptionsLen);
  uint8_t sampled = 0;
  if (options[0] == ';' && options[1] == 'o' && options[2] == '=') {
    if (options.back() < '0' || options.back() > '3') {
      return invalid;  // Invalid option.
    }
    if (options.back() == '1' || options.back() == '3') {
      sampled = 1;
    }
    // Remove from header to make parsing span_id easier.
    header = header.substr(0, header.size() - kOptionsLen);
  }

  // Parse decimal span_id.
  absl::string_view span_id = header.substr(kTraceIdLenHex + 1);
  uint64_t n_span_id;
  if (!absl::SimpleAtoi(span_id, &n_span_id) || n_span_id == 0) {
    return invalid;  // Invalid span_id.
  }

  // Parse trace_id.
  absl::string_view trace_id = header.substr(0, kTraceIdLenHex);
  if (!IsHexDigits(trace_id)) {
    return invalid;  // Invalid hex digit.
  }
  std::string trace_id_binary = absl::HexStringToBytes(trace_id);

  return SpanContext(
      TraceId(reinterpret_cast<const uint8_t*>(trace_id_binary.data())),
      FromDecimal(n_span_id), TraceOptions(&sampled));
}

std::string ToCloudTraceContextHeader(const SpanContext& ctx) {
  return absl::StrCat(ctx.trace_id().ToHex(), "/", ToDecimal(ctx.span_id()),
                      ctx.trace_options().IsSampled() ? ";o=1" : ";o=0");
}

}  // namespace propagation
}  // namespace trace
}  // namespace opencensus
