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

#include "opencensus/trace/context_util.h"

#include "gtest/gtest.h"
#include "opencensus/context/context.h"
#include "opencensus/trace/span.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/with_span.h"

// Not in namespace ::opencensus::context in order to better reflect what user
// code should look like.

namespace {

TEST(FromContextTest, GetCurrentSpan) {
  opencensus::trace::SpanContext zeroed_span_context;
  auto span = opencensus::trace::Span::StartSpan("MySpan");
  EXPECT_EQ(zeroed_span_context, opencensus::trace::GetCurrentSpan().context());
  {
    opencensus::trace::WithSpan ws(span);
    EXPECT_EQ(span.context(), opencensus::trace::GetCurrentSpan().context());
  }
  span.End();
}

TEST(FromContextTest, GetSpanFromContext) {
  opencensus::trace::SpanContext zeroed_span_context;
  auto span = opencensus::trace::Span::StartSpan("MySpan");
  opencensus::context::Context ctx = opencensus::context::Context::Current();
  EXPECT_EQ(zeroed_span_context,
            opencensus::trace::GetSpanFromContext(ctx).context());
  {
    opencensus::trace::WithSpan ws(span);
    ctx = opencensus::context::Context::Current();
  }
  EXPECT_EQ(zeroed_span_context, opencensus::trace::GetCurrentSpan().context());
  EXPECT_EQ(span.context(),
            opencensus::trace::GetSpanFromContext(ctx).context());
  span.End();
}

}  // namespace
