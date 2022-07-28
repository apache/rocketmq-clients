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

TEST(B3Test, Trace128Span64Valid) {
  SpanContext ctx = FromB3Headers("463ac35c9f6413ad48485a3953bb6124",
                                  "0020000000000001", "1", "");
  EXPECT_THAT(ctx, IsValid());
  EXPECT_EQ("463ac35c9f6413ad48485a3953bb6124-0020000000000001-01",
            ctx.ToString());
  // Round-trip.
  EXPECT_EQ("463ac35c9f6413ad48485a3953bb6124", ToB3TraceIdHeader(ctx));
  EXPECT_EQ("0020000000000001", ToB3SpanIdHeader(ctx));
  EXPECT_EQ("1", ToB3SampledHeader(ctx));
}

TEST(B3Test, Trace64Span64Valid) {
  SpanContext ctx =
      FromB3Headers("1234567812345678", "0020000000000001", "1", "");
  EXPECT_THAT(ctx, IsValid());
  EXPECT_EQ("00000000000000001234567812345678-0020000000000001-01",
            ctx.ToString());
  // Round-trip.
  EXPECT_EQ("00000000000000001234567812345678", ToB3TraceIdHeader(ctx));
  EXPECT_EQ("0020000000000001", ToB3SpanIdHeader(ctx));
  EXPECT_EQ("1", ToB3SampledHeader(ctx));
}

TEST(B3Test, NotSampled) {
  SpanContext ctx = FromB3Headers("463ac35c9f6413ad48485a3953bb6124",
                                  "0020000000000001", "0", "");
  EXPECT_THAT(ctx, IsValid());
  EXPECT_EQ("463ac35c9f6413ad48485a3953bb6124-0020000000000001-00",
            ctx.ToString());
  // Round-trip.
  EXPECT_EQ("463ac35c9f6413ad48485a3953bb6124", ToB3TraceIdHeader(ctx));
  EXPECT_EQ("0020000000000001", ToB3SpanIdHeader(ctx));
  EXPECT_EQ("0", ToB3SampledHeader(ctx));
}

TEST(B3Test, DebugOverridesNotSampled) {
  SpanContext ctx = FromB3Headers("463ac35c9f6413ad48485a3953bb6124",
                                  "0020000000000001", "0", "1");
  EXPECT_THAT(ctx, IsValid());
  EXPECT_EQ("463ac35c9f6413ad48485a3953bb6124-0020000000000001-01",
            ctx.ToString());
  // Round-trip.
  EXPECT_EQ("463ac35c9f6413ad48485a3953bb6124", ToB3TraceIdHeader(ctx));
  EXPECT_EQ("0020000000000001", ToB3SpanIdHeader(ctx));
  EXPECT_EQ("1", ToB3SampledHeader(ctx));
}

TEST(B3Test, ExpectedFailures) {
#define INVALID(a, b, c, d) EXPECT_THAT(FromB3Headers(a, b, c, d), IsInvalid())
  INVALID("", "", "", "");
  INVALID("463ac35c9f6413ad48485a3953bb612", "0020000000000001", "1", "")
      << "trace_id too short for 128-bit";
  INVALID("463ac35c9f6413ad48485a3953bb61245", "0020000000000001", "1", "")
      << "trace_id too long for 128-bit";
  INVALID("463ac35c9f6413ad48485a3953bb612X", "0020000000000001", "1", "")
      << "trace_id not hex";

  INVALID("48485a3953bb612", "0020000000000001", "1", "")
      << "trace_id too short for 64-bit";
  INVALID("48485a3953bb61245", "0020000000000001", "1", "")
      << "trace_id too long for 64-bit";
  INVALID("48485a3953bb612X", "0020000000000001", "1", "")
      << "trace_id not hex";

  INVALID("48485a3953bb6124", "002000000000000", "1", "")
      << "span_id too short";
  INVALID("48485a3953bb6124", "00200000000000013", "1", "")
      << "span_id too long";
  INVALID("48485a3953bb6124", "002000000000000X", "1", "") << "span_id not hex";

  INVALID("48485a3953bb6124", "0020000000000001", "hello", "")
      << "sampled set to bad value";

  INVALID("48485a3953bb6124", "0020000000000001", "", "hello")
      << "flags set to bad value";

  INVALID("00000000000000000000000000000000", "0020000000000001", "1", "")
      << "zero is an invalid trace_id";
  INVALID("463ac35c9f6413ad48485a3953bb6124", "0000000000000000", "1", "")
      << "zero is an invalid span_id";
#undef INVALID
}

}  // namespace
}  // namespace propagation
}  // namespace trace
}  // namespace opencensus
