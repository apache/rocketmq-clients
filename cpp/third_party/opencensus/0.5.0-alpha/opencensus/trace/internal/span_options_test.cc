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

#include <memory>

#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "gtest/gtest.h"
#include "opencensus/trace/exporter/message_event.h"
#include "opencensus/trace/exporter/status.h"
#include "opencensus/trace/internal/span_impl.h"
#include "opencensus/trace/sampler.h"
#include "opencensus/trace/span.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/status_code.h"
#include "opencensus/trace/trace_options.h"
#include "opencensus/trace/trace_params.h"

namespace opencensus {
namespace trace {

// Helper class to peer into SpanImpl implementation details.
class SpanTestPeer {
 public:
  static exporter::Status GetStatus(Span* span) {
    // This is a hack so that we don't have to add a status() accessor to the
    // public API.
    absl::MutexLock l(&span->span_impl_for_test()->mu_);
    return span->span_impl_for_test()->status_;
  }

  static std::string GetName(Span* span) {
    return span->span_impl_for_test()->name();
  }

  static SpanId GetParentSpanId(Span* span) {
    return span->span_impl_for_test()->parent_span_id();
  }
};

namespace {

TEST(SpanTest, StartSpanOptions) {
  AlwaysSampler sampler;
  auto span = Span::StartSpan("test_span", nullptr, {&sampler});
  EXPECT_EQ("test_span", SpanTestPeer::GetName(&span));
  EXPECT_TRUE(span.context().IsValid());
  EXPECT_TRUE(span.IsSampled());
  EXPECT_EQ(SpanId(), SpanTestPeer::GetParentSpanId(&span));
}

TEST(SpanTest, EndAndStatus) {
  AlwaysSampler sampler;
  auto span = Span::StartSpan("test_span", nullptr, {&sampler});
  span.SetStatus(StatusCode::UNKNOWN, "error description");
  span.End();
  span.SetStatus(StatusCode::OK, "can't set status after end");

  exporter::Status end_status = SpanTestPeer::GetStatus(&span);
  EXPECT_EQ(StatusCode::UNKNOWN, end_status.CanonicalCode());
}

}  // namespace
}  // namespace trace
}  // namespace opencensus
