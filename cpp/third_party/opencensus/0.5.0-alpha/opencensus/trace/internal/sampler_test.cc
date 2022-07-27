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

#include "opencensus/trace/sampler.h"

#include <atomic>

#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "gtest/gtest.h"
#include "opencensus/trace/span.h"
#include "opencensus/trace/trace_params.h"

namespace opencensus {
namespace trace {
namespace {

// Example of a stateful sampler class.
class SampleEveryNth : public Sampler {
 public:
  explicit SampleEveryNth(int nth) : state_(new State(nth)) {}

  bool ShouldSample(const SpanContext* parent_context, bool has_remote_parent,
                    const TraceId& trace_id, const SpanId& span_id,
                    absl::string_view name,
                    const std::vector<Span*>& parent_links) const override {
    // The shared_ptr is const, but the underlying State it points to isn't.
    return state_->Increment();
  }

 private:
  class State {
   public:
    explicit State(int nth) : nth_(nth), current_(0) {}
    bool Increment() {
      int prev = current_.fetch_add(1, std::memory_order_acq_rel);
      return (prev + 1) % nth_ == 0;
    }

   private:
    const int nth_;
    std::atomic<int> current_;
  };

  std::shared_ptr<State> state_;
};

TEST(SamplerTest, SampleNth) {
  static constexpr int kSampleRate = 4;
  SampleEveryNth sampler(kSampleRate);

  for (int i = 1; i <= 100; ++i) {
    auto span = Span::StartSpan(absl::StrCat("MySpan", i), nullptr, {&sampler});
    if (i % kSampleRate == 0) {
      EXPECT_TRUE(span.IsSampled());
    } else {
      EXPECT_FALSE(span.IsSampled());
    }
  }
}

}  // namespace
}  // namespace trace
}  // namespace opencensus
