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

TEST(CloudTraceContextTest, ParseFull) {
  constexpr char header[] = "01020304050607081112131415161718/123;o=1";
  SpanContext ctx = FromCloudTraceContextHeader(header);
  EXPECT_THAT(ctx, IsValid());
  EXPECT_EQ("01020304050607081112131415161718-000000000000007b-01",
            ctx.ToString());
  EXPECT_EQ(header, ToCloudTraceContextHeader(ctx));
}

TEST(CloudTraceContextTest, ParseNoOptions) {
  constexpr char header[] = "01020304050607081112131415161718/456";
  SpanContext ctx = FromCloudTraceContextHeader(header);
  EXPECT_THAT(ctx, IsValid());
  EXPECT_EQ("01020304050607081112131415161718-00000000000001c8-00",
            ctx.ToString());
  EXPECT_EQ("01020304050607081112131415161718/456;o=0",
            ToCloudTraceContextHeader(ctx))
      << "missing option is canonicalized to o=0";
}

TEST(CloudTraceContextTest, ParseMaxValues) {
  constexpr char header[] =
      "ffffffffffffffffffffffffffffffff/18446744073709551615;o=3";
  SpanContext ctx = FromCloudTraceContextHeader(header);
  EXPECT_THAT(ctx, IsValid());
  EXPECT_EQ("ffffffffffffffffffffffffffffffff-ffffffffffffffff-01",
            ctx.ToString());
  EXPECT_EQ("ffffffffffffffffffffffffffffffff/18446744073709551615;o=1",
            ToCloudTraceContextHeader(ctx))
      << "o=3 is canonicalized to o=1";
}

TEST(CloudTraceContextTest, ExpectedFailures) {
#define INVALID(str) EXPECT_THAT(FromCloudTraceContextHeader(str), IsInvalid())
  INVALID("");
  INVALID("1234567890123456789012345678901") << "too short.";
  INVALID("12345678901234567890123456789012") << "too short.";
  INVALID("12345678901234567890123456789012/") << "too short.";
  INVALID("1xyz5678901234567890123456789012/1") << "trace_id not hex.";
  INVALID("123456789012345678901234567890123/1") << "trace_id too long.";
  INVALID("12345678901234567890123456789012/;o=1") << "missing span_id.";
  INVALID("12345678901234567890123456789012/1xy2;o=1")
      << "span_id must be decimal.";
  INVALID("12345678901234567890123456789012/123/123") << "too many slashes.";
  INVALID("12345678901234567890123456789012/18446744073709551617;o=1")
      << "span_id is too large. (uint64max + 1)";
  INVALID("12345678901234567890123456789012/456;") << "missing options.";
  INVALID("12345678901234567890123456789012/456;o=")
      << "missing options value.";
  INVALID("12345678901234567890123456789012/456;o=4")
      << "options value too large.";
  INVALID("12345678901234567890123456789012/456;o01") << "malformed options.";
  INVALID("12345678901234567890123456789012/456;o1") << "malformed options.";
  INVALID("00000000000000000000000000000000/456;o=1")
      << "zero is an invalid trace_id.";
  INVALID("12345678901234567890123456789012/0;o=1")
      << "zero is an invalid span_id.";
  INVALID("12345678901234567890123456789012/-123;o=1")
      << "negative span_id is invalid.";
#undef INVALID
}

}  // namespace
}  // namespace propagation
}  // namespace trace
}  // namespace opencensus
