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

#include "opencensus/trace/exporter/span_exporter.h"

#include "absl/memory/memory.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "opencensus/trace/exporter/span_data.h"
#include "opencensus/trace/sampler.h"
#include "opencensus/trace/span.h"

namespace opencensus {
namespace trace {
namespace {

class Counter {
 public:
  static Counter* Get() {
    static Counter* global_counter = new Counter;
    return global_counter;
  }

  void Increment(int n) {
    absl::MutexLock l(&mu_);
    value_ += n;
  }

  int value() const {
    absl::MutexLock l(&mu_);
    return value_;
  }

 private:
  Counter() = default;
  mutable absl::Mutex mu_;
  int value_ ABSL_GUARDED_BY(mu_) = 0;
};

class MyExporter : public exporter::SpanExporter::Handler {
 public:
  static void Register() {
    exporter::SpanExporter::RegisterHandler(absl::make_unique<MyExporter>());
  }

  void Export(const std::vector<exporter::SpanData>& spans) override {
    Counter::Get()->Increment(spans.size());
  }
};

class SpanExporterTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() {
    exporter::SpanExporter::SetBatchSize(1);
    exporter::SpanExporter::SetInterval(absl::Seconds(1));

    // Only register once.
    MyExporter::Register();
  }
};

TEST_F(SpanExporterTest, BasicExportTest) {
  ::opencensus::trace::AlwaysSampler sampler;
  ::opencensus::trace::StartSpanOptions opts = {&sampler};
  auto span1 = ::opencensus::trace::Span::StartSpan("Span1", nullptr, opts);
  absl::SleepFor(absl::Milliseconds(10));
  auto span2 = ::opencensus::trace::Span::StartSpan("Span2", &span1, opts);
  absl::SleepFor(absl::Milliseconds(20));
  auto span3 = ::opencensus::trace::Span::StartSpan("Span3", &span2);
  absl::SleepFor(absl::Milliseconds(30));
  span3.End();
  span2.End();
  span1.End();

  for (int i = 0; i < 10; ++i) {
    if (Counter::Get()->value() >= 3) break;
    absl::SleepFor(absl::Seconds(1));
  }
  EXPECT_EQ(3, Counter::Get()->value());
}

}  // namespace
}  // namespace trace
}  // namespace opencensus
