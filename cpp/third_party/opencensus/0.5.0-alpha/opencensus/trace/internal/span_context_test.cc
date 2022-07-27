// Copyright 2017, OpenCensus Authors
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

#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_id.h"

#include "gtest/gtest.h"

namespace opencensus {
namespace trace {
namespace {

constexpr uint8_t trace_id[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
constexpr uint8_t span_id[] = {10, 2, 3, 4, 5, 6, 7, 8};

TEST(SpanContextTest, IsValid) {
  SpanContext ctx1;
  SpanContext ctx2{TraceId(trace_id), SpanId(span_id)};
  SpanContext ctx3{TraceId(), SpanId(span_id)};
  SpanContext ctx4{TraceId(trace_id), SpanId()};

  EXPECT_FALSE(ctx1.IsValid()) << "A blank context shouldn't be valid.";
  EXPECT_TRUE(ctx2.IsValid());
  EXPECT_FALSE(ctx3.IsValid());
  EXPECT_FALSE(ctx4.IsValid());
}

TEST(SpanContextTest, Equality) {
  constexpr uint8_t span_id2[] = {1, 20, 3, 4, 5, 6, 7, 8};
  constexpr uint8_t opts[] = {1};

  SpanContext ctx1{TraceId(trace_id), SpanId(span_id)};
  SpanContext ctx2{TraceId(trace_id), SpanId(span_id2)};
  SpanContext ctx3{TraceId(trace_id), SpanId(span_id)};
  SpanContext ctx4{TraceId(trace_id), SpanId(span_id), TraceOptions(opts)};

  EXPECT_EQ(ctx1, ctx1);
  EXPECT_NE(ctx1, ctx2);
  EXPECT_EQ(ctx1, ctx3) << "Same contents, different objects.";
  EXPECT_EQ(ctx1, ctx4) << "Comparison ignores options.";
  EXPECT_FALSE(ctx1 == ctx2);
}

TEST(SpanContextTest, ToString) {
  constexpr uint8_t opts[] = {1};
  SpanContext ctx{TraceId(trace_id), SpanId(span_id), TraceOptions(opts)};
  EXPECT_EQ("01020304050607080900010203040506-0a02030405060708-01",
            ctx.ToString());
}

}  // namespace
}  // namespace trace
}  // namespace opencensus
