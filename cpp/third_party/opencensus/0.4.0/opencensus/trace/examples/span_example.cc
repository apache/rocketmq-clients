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

#include <functional>
#include <memory>
#include <thread>

#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "opencensus/trace/exporter/status.h"
#include "opencensus/trace/sampler.h"
#include "opencensus/trace/span.h"

namespace {

TEST(SpanExample, ParentAndChildSpans) {
  auto root_span = ::opencensus::trace::Span::StartSpan("MyRootSpan");
  root_span.AddAnnotation(
      "I'm the root span and I'm about to create a child span.");
  auto child_span = ::opencensus::trace::Span::StartSpan("MyChildSpan",
                                                         /*parent=*/&root_span);
  EXPECT_EQ(root_span.context().trace_id(), child_span.context().trace_id());
  child_span.AddAnnotation("Hello, I'm the child span.");
  child_span.AddAttribute("example.com/my_key", "my value");
  child_span.AddAttributes(
      {{"example.com/a_number", 123}, {"example.com/is_present", true}});
  child_span.SetStatus(::opencensus::trace::StatusCode::INVALID_ARGUMENT,
                       "Example error - pretend MyChildSpan had bad inputs.");
  child_span.End();
  root_span.AddAnnotation(
      "I'm the root span again and my child span has just ended.");
  root_span.End();  // Implies status is OK.
}

TEST(SpanExample, AlwaysSample) {
  opencensus::trace::AlwaysSampler sampler;
  auto span = ::opencensus::trace::Span::StartSpan(
      "MyRootSpan", /*parent=*/nullptr, {&sampler});
  span.End();
}

void PretendCallback(::opencensus::trace::Span&& s) {
  s.AddAnnotation("Performing work.");
  s.End();
}

TEST(SpanExample, MoveSpan) {
  auto span = ::opencensus::trace::Span::StartSpan("OperationSpan");
  span.AddAnnotation("About to defer work.");
  // This work could be scheduled on a thread pool or deferred until after an
  // async operation completes.
  std::function<void()> callback = [&span] {
    PretendCallback(std::move(span));
  };
  callback();
}

void ParallelWorker(::opencensus::trace::Span* span, int work_unit) {
  span->AddAnnotation(
      absl::StrCat("Doing work unit ", work_unit,
                   " on behalf of currently installed context."));
}

TEST(SpanExample, PointerToSpan) {
  auto op_span = ::opencensus::trace::Span::StartSpan("OperationSpan");
  {
    std::thread t1(ParallelWorker, &op_span, 123);
    std::thread t2(ParallelWorker, &op_span, 456);
    t1.join();
    t2.join();
  }
  op_span.End();
}

}  // namespace
