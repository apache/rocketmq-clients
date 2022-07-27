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

#include "opencensus/trace/propagation/trace_context.h"

#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_id.h"
#include "opencensus/trace/trace_options.h"

#include "absl/strings/ascii.h"
#include "absl/strings/escaping.h"
#include "absl/strings/str_cat.h"

namespace opencensus {
namespace trace {
namespace propagation {

namespace {

// Returns true if the string only contains valid lowercase hex digits.
bool IsLowercaseHexDigits(absl::string_view s) {
  for (int i = 0; i < s.length(); ++i) {
    if (!((s[i] >= '0' && s[i] <= '9') || (s[i] >= 'a' && s[i] <= 'f'))) {
      return false;
    }
  }
  return true;
}

// Returns true if the converted string was valid hex.
bool FromHex(absl::string_view hex, std::string* bin) {
  if (!IsLowercaseHexDigits(hex)) return false;
  *bin = absl::HexStringToBytes(hex);
  return true;
}

}  // namespace

SpanContext FromTraceParentHeader(absl::string_view header) {
  constexpr int kDelimiterLen = 1;
  constexpr char kDelimiter = '-';
  constexpr int kVersionLen = 1;
  constexpr int kTraceIdLen = 16;
  constexpr int kSpanIdLen = 8;
  constexpr int kTraceOptionsLen = 1;
  constexpr int kVersionLenHex = 2 * kVersionLen;
  constexpr int kTraceIdLenHex = 2 * kTraceIdLen;
  constexpr int kSpanIdLenHex = 2 * kSpanIdLen;
  constexpr int kTraceOptionsLenHex = 2 * kTraceOptionsLen;
  constexpr int kTotalLenInHexDigits = kVersionLenHex + kTraceIdLenHex +
                                       kSpanIdLenHex + kTraceOptionsLenHex +
                                       3 * kDelimiterLen;
  constexpr int kVersionOfs = 0;
  constexpr int kTraceIdOfs = kVersionOfs + kVersionLenHex + kDelimiterLen;
  constexpr int kSpanIdOfs = kTraceIdOfs + kTraceIdLenHex + kDelimiterLen;
  constexpr int kOptionsOfs = kSpanIdOfs + kSpanIdLenHex + kDelimiterLen;
  static_assert(kOptionsOfs + kTraceOptionsLenHex == kTotalLenInHexDigits,
                "bad offsets");
  static SpanContext invalid;
  if (header.size() != kTotalLenInHexDigits || header[kVersionOfs] != '0' ||
      header[kVersionOfs + 1] != '0' ||
      header[kTraceIdOfs - kDelimiterLen] != kDelimiter ||
      header[kSpanIdOfs - kDelimiterLen] != kDelimiter ||
      header[kOptionsOfs - kDelimiterLen] != kDelimiter) {
    return invalid;  // Invalid length, version or format.
  }
  std::string trace_id_bin, span_id_bin, options_bin;
  if (!FromHex(header.substr(kTraceIdOfs, kTraceIdLenHex), &trace_id_bin) ||
      !FromHex(header.substr(kSpanIdOfs, kSpanIdLenHex), &span_id_bin) ||
      !FromHex(header.substr(kOptionsOfs, kTraceOptionsLenHex), &options_bin)) {
    return invalid;  // Invalid hex.
  }
  return SpanContext(
      TraceId(reinterpret_cast<const uint8_t*>(trace_id_bin.data())),
      SpanId(reinterpret_cast<const uint8_t*>(span_id_bin.data())),
      TraceOptions(reinterpret_cast<const uint8_t*>(options_bin.data())));
}

std::string ToTraceParentHeader(const SpanContext& ctx) {
  return absl::StrCat("00-", ctx.ToString());
}

}  // namespace propagation
}  // namespace trace
}  // namespace opencensus
