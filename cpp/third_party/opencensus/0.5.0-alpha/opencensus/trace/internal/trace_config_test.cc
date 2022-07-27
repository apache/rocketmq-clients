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

#include "opencensus/trace/trace_config.h"

#include <atomic>
#include <thread>

#include "absl/time/clock.h"
#include "gtest/gtest.h"
#include "opencensus/trace/span.h"
#include "opencensus/trace/trace_params.h"

namespace opencensus {
namespace trace {
namespace {

TEST(TraceConfigTest, MultiThreaded) {
  std::atomic<bool> running(true);

  auto set_current_trace_params = [&running] {
    int n = 0;
    constexpr int denom = 16;
    while (running) {
      n = (n + 1) % (denom + 1);
      TraceConfig::SetCurrentTraceParams(
          {128, 128, 64, 64,
           ProbabilitySampler(n / static_cast<double>(denom))});
    }
  };

  auto make_spans = [&running] {
    while (running) {
      auto span = Span::StartSpan("MySpan");
      span.AddAnnotation("Hello world.");
      span.End();
    }
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < 16; ++i) {
    threads.emplace_back(set_current_trace_params);
    threads.emplace_back(make_spans);
  }

  absl::SleepFor(absl::Seconds(1));
  running = false;

  for (auto& thread : threads) {
    thread.join();
  }
}

}  // namespace
}  // namespace trace
}  // namespace opencensus
