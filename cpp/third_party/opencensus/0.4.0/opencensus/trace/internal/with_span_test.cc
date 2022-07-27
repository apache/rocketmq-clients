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

#include "opencensus/trace/with_span.h"

#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "opencensus/context/context.h"
#include "opencensus/trace/span.h"
#include "opencensus/trace/span_context.h"

namespace opencensus {
namespace context {
class ContextTestPeer {
 public:
  static const opencensus::trace::SpanContext& CurrentCtx() {
    return Context::InternalMutableCurrent()->span_.context();
  }
};
}  // namespace context
}  // namespace opencensus

// Not in namespace ::opencensus::context in order to better reflect what user
// code should look like.

namespace {

using opencensus::context::ContextTestPeer;

void LogCurrentContext() {
  const std::string s = opencensus::context::Context::Current().DebugString();
  std::cout << "  current: " << s << "\n";
}

void ExpectNoSpan() {
  opencensus::trace::SpanContext zeroed_span_context;
  EXPECT_EQ(zeroed_span_context, ContextTestPeer::CurrentCtx());
}

TEST(WithSpanTest, NoSpanInDefaultContext) { ExpectNoSpan(); }

TEST(WithSpanTest, SpanIsAddedAndRemoved) {
  auto span = opencensus::trace::Span::StartSpan("MySpan");
  ExpectNoSpan();
  {
    opencensus::trace::WithSpan ws(span);
    LogCurrentContext();
    EXPECT_EQ(span.context(), ContextTestPeer::CurrentCtx())
        << "Span was installed.";
  }
  ExpectNoSpan();
  span.End();
}

TEST(WithSpanTest, Nesting) {
  auto span1 = opencensus::trace::Span::StartSpan("MySpan1");
  auto span2 = opencensus::trace::Span::StartSpan("MySpan2");
  ExpectNoSpan();
  {
    opencensus::trace::WithSpan ws1(span1);
    EXPECT_EQ(span1.context(), ContextTestPeer::CurrentCtx())
        << "Span1 was installed.";
    {
      opencensus::trace::WithSpan ws2(span2);
      EXPECT_EQ(span2.context(), ContextTestPeer::CurrentCtx())
          << "Span2 was installed.";
    }
    EXPECT_EQ(span1.context(), ContextTestPeer::CurrentCtx())
        << "Span2 was uninstalled, back to Span1.";
  }
  ExpectNoSpan();
  span2.End();
  span1.End();
}

TEST(WithSpanTest, DisabledViaConditional) {
  auto span1 = opencensus::trace::Span::StartSpan("MySpan1");
  auto span2 = opencensus::trace::Span::StartSpan("MySpan2");
  ExpectNoSpan();
  {
    opencensus::trace::WithSpan ws1(span1);
    EXPECT_EQ(span1.context(), ContextTestPeer::CurrentCtx())
        << "Span1 was installed.";
    {
      opencensus::trace::WithSpan ws2(span2, false);
      EXPECT_EQ(span1.context(), ContextTestPeer::CurrentCtx())
          << "Span2 was NOT installed, blocked by condition.";
    }
    EXPECT_EQ(span1.context(), ContextTestPeer::CurrentCtx())
        << "Still Span1 after WithSpan2 goes out of scope.";
  }
  ExpectNoSpan();
  span2.End();
  span1.End();
}

TEST(WithSpanTest, EndSpan) {
  auto span = opencensus::trace::Span::StartSpan("MySpan");
  ExpectNoSpan();
  {
    opencensus::trace::WithSpan ws(span, /*cond=*/true, /*end_span=*/true);
    EXPECT_EQ(span.context(), ContextTestPeer::CurrentCtx());
  }
  ExpectNoSpan();
  // TODO: Check End() was called.
}

#ifndef NDEBUG
TEST(WithSpanDeathTest, DestructorOnWrongThread) {
  auto span = opencensus::trace::Span::StartSpan("MySpan");
  EXPECT_DEBUG_DEATH(
      {
        auto* ws = new opencensus::trace::WithSpan(span);
        std::thread t([ws]() {
          // Running the destructor in a different thread corrupts its
          // thread-local Context. In debug mode, it assert()s.
          delete ws;
        });
        t.join();
      },
      "must be destructed on the same thread");
}
#endif

}  // namespace
