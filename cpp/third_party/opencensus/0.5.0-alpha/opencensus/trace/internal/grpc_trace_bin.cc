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

#include <cstdint>

#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_id.h"
#include "opencensus/trace/trace_options.h"

namespace opencensus {
namespace trace {
namespace propagation {

namespace {

constexpr int kVersionLen = 1;
constexpr int kTraceIdLen = 16;
constexpr int kSpanIdLen = 8;
constexpr int kTraceOptionsLen = 1;

constexpr uint8_t kVersionId = 0;
constexpr uint8_t kTraceIdField = 0;
constexpr uint8_t kSpanIdField = 1;
constexpr uint8_t kTraceOptionsField = 2;

constexpr int kVersionOfs = 0;
constexpr int kTraceIdOfs = 1;
constexpr int kSpanIdOfs = kTraceIdOfs + 1 + kTraceIdLen;
constexpr int kTraceOptionsOfs = kSpanIdOfs + 1 + kSpanIdLen;

static_assert(kVersionLen + 1 + kTraceIdLen + 1 + kSpanIdLen + 1 +
                      kTraceOptionsLen ==
                  kGrpcTraceBinHeaderLen,
              "header length is wrong");

}  // namespace

SpanContext FromGrpcTraceBinHeader(absl::string_view header) {
  if (header.size() < kGrpcTraceBinHeaderLen ||
      header[kVersionOfs] != kVersionId ||
      header[kTraceIdOfs] != kTraceIdField ||
      header[kSpanIdOfs] != kSpanIdField ||
      header[kTraceOptionsOfs] != kTraceOptionsField) {
    return SpanContext();  // Invalid.
  }

  uint8_t options = header[kTraceOptionsOfs + 1] & 1;

  return SpanContext(
      TraceId(reinterpret_cast<const uint8_t*>(&header[kTraceIdOfs + 1])),
      SpanId(reinterpret_cast<const uint8_t*>(&header[kSpanIdOfs + 1])),
      TraceOptions(&options));
}

std::string ToGrpcTraceBinHeader(const SpanContext& ctx) {
  std::string out(kGrpcTraceBinHeaderLen, '\0');
  ToGrpcTraceBinHeader(ctx, reinterpret_cast<uint8_t*>(&out[0]));
  return out;
}

void ToGrpcTraceBinHeader(const SpanContext& ctx, uint8_t* out) {
  out[kVersionOfs] = kVersionId;
  out[kTraceIdOfs] = kTraceIdField;
  ctx.trace_id().CopyTo(reinterpret_cast<uint8_t*>(&out[kTraceIdOfs + 1]));
  out[kSpanIdOfs] = kSpanIdField;
  ctx.span_id().CopyTo(reinterpret_cast<uint8_t*>(&out[kSpanIdOfs + 1]));
  out[kTraceOptionsOfs] = kTraceOptionsField;
  ctx.trace_options().CopyTo(
      reinterpret_cast<uint8_t*>(&out[kTraceOptionsOfs + 1]));
}

}  // namespace propagation
}  // namespace trace
}  // namespace opencensus
