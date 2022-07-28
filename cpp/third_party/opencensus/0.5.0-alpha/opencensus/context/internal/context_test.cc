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

#include "opencensus/context/context.h"

#include <functional>
#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "opencensus/tags/context_util.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/tags/tag_map.h"
#include "opencensus/tags/with_tag_map.h"
#include "opencensus/trace/context_util.h"
#include "opencensus/trace/span.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/with_span.h"

// Not in namespace ::opencensus::context in order to better reflect what user
// code should look like.

namespace {

void LogCurrentContext() {
  const std::string s = opencensus::context::Context::Current().DebugString();
  std::cout << "  current: " << s << "\n";
}

void ExpectEmptyContext() {
  EXPECT_TRUE(opencensus::tags::GetCurrentTagMap().tags().empty());
  opencensus::trace::SpanContext zeroed_span_context;
  EXPECT_EQ(zeroed_span_context, opencensus::trace::GetCurrentSpan().context());
}

opencensus::tags::TagMap ExampleTagMap() {
  static const auto k1 = opencensus::tags::TagKey::Register("key1");
  static const auto k2 = opencensus::tags::TagKey::Register("key2");
  return opencensus::tags::TagMap({{k1, "v1"}, {k2, "v2"}});
}

TEST(ContextTest, DefaultContext) {
  LogCurrentContext();
  ExpectEmptyContext();
}

void Callback1(const opencensus::trace::Span& expected_span) {
  EXPECT_EQ(ExampleTagMap(), opencensus::tags::GetCurrentTagMap());
  EXPECT_EQ(expected_span.context(),
            opencensus::trace::GetCurrentSpan().context());
}

TEST(ContextTest, Wrap) {
  auto span = opencensus::trace::Span::StartSpan("MySpan");
  std::function<void()> fn;
  {
    opencensus::tags::WithTagMap wt(ExampleTagMap());
    opencensus::trace::WithSpan ws(span);
    fn = opencensus::context::Context::Current().Wrap(
        [span]() { Callback1(span); });
  }
  ExpectEmptyContext();
  fn();
  span.End();
}

TEST(ContextTest, WrapDoesNotLeak) {
  // Leak-sanitizer (part of ASAN) throws an error if this leaks.
  auto span = opencensus::trace::Span::StartSpan("MySpan");
  {
    opencensus::tags::WithTagMap wt(ExampleTagMap());
    opencensus::trace::WithSpan ws(span);
    std::function<void()> fn = opencensus::context::Context::Current().Wrap(
        [span]() { Callback1(span); });
  }
  // We never call fn().
  span.End();
}

TEST(ContextTest, ThreadDoesNotLeak) {
  auto span = opencensus::trace::Span::StartSpan("MySpan");
  std::thread t([span]() {
    // Leak-sanitizer (part of ASAN) throws an error if this leaks.
    opencensus::tags::WithTagMap wt(ExampleTagMap());
    opencensus::trace::WithSpan ws(span);
  });
  t.join();
  ExpectEmptyContext();
  span.End();
}

TEST(ContextTest, WrappedFnIsCopiable) {
  std::function<void()> fn1, fn2;
  {
    opencensus::tags::WithTagMap wt(ExampleTagMap());
    fn1 = opencensus::context::Context::Current().Wrap(
        []() { Callback1(opencensus::trace::Span::BlankSpan()); });
    fn2 = fn1;
    fn1();
  }
  fn2();
}

}  // namespace
