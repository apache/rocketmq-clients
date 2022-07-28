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

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_id.h"

namespace opencensus {
namespace trace {
namespace propagation {
namespace {

MATCHER(IsValid, "is a valid SpanContext") { return arg.IsValid(); }
MATCHER(IsInvalid, "is an invalid SpanContext") { return !arg.IsValid(); }

TEST(TraceParentTest, ParseFull) {
  constexpr char header[] =
      "00-404142434445464748494a4b4c4d4e4f-6162636465666768-01";
  SpanContext ctx = FromTraceParentHeader(header);
  EXPECT_THAT(ctx, IsValid());
  EXPECT_EQ("404142434445464748494a4b4c4d4e4f-6162636465666768-01",
            ctx.ToString());
  EXPECT_EQ(header, ToTraceParentHeader(ctx));
}

TEST(TraceParentTest, ParseTraceDisabled) {
  constexpr char header[] =
      "00-404142434445464748494a4b4c4d4e4f-6162636465666768-00";
  SpanContext ctx = FromTraceParentHeader(header);
  EXPECT_THAT(ctx, IsValid());
  EXPECT_EQ("404142434445464748494a4b4c4d4e4f-6162636465666768-00",
            ctx.ToString());
  EXPECT_EQ(header, ToTraceParentHeader(ctx));
}

TEST(TraceParentTest, ExpectedFailures) {
#define INVALID(str) EXPECT_THAT(FromTraceParentHeader(str), IsInvalid())
  INVALID("");
  INVALID("12-404142434445464748494a4b4c4d4e4f-6162636465666768-00")
      << "unknown version.";
  INVALID("00-404142434445464748494a4b4c4d4e4f-6162636465666768-0")
      << "too short.";
  INVALID("00-404142434445464748494a4b4c4d4e4f-6162636465666768-011")
      << "too long.";
  INVALID("xy-404142434445464748494a4b4c4d4e4f-6162636465666768-01")
      << "version not hex.";
  INVALID("00-4x4y42434445464748494a4b4c4d4e4f-6162636465666768-01")
      << "trace_id not hex.";
  INVALID("00-404142434445464748494a4b4c4d4e4f-6x6y636465666768-01")
      << "span_id not hex.";
  INVALID("00-404142434445464748494a4b4c4d4e4f-6162636465666768-xy")
      << "options not hex.";
  INVALID("00-00000000000000000000000000000000-6162636465666768-01")
      << "trace_id can't be zero.";
  INVALID("00-404142434445464748494a4b4c4d4e4f-0000000000000000-01")
      << "span_id can't be zero.";
  INVALID("00_404142434445464748494a4b4c4d4e4f-6162636465666768-01")
      << "invalid delimiter.";
  INVALID("00-404142434445464748494a4b4c4d4e4f_6162636465666768-01")
      << "invalid delimiter.";
  INVALID("00-404142434445464748494a4b4c4d4e4f-6162636465666768_01")
      << "invalid delimiter.";
  INVALID("00-404142434445464748494A4B4C4D4E4F-6162636465666768-01")
      << "uppercase is invalid.";
#undef INVALID
}

}  // namespace
}  // namespace propagation
}  // namespace trace
}  // namespace opencensus
