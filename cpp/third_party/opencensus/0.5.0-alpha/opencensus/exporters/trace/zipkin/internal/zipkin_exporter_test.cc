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

#include "opencensus/exporters/trace/zipkin/zipkin_exporter.h"

#include "absl/time/clock.h"
#include "gtest/gtest.h"
#include "opencensus/trace/exporter/annotation.h"
#include "opencensus/trace/internal/local_span_store.h"
#include "opencensus/trace/span.h"

namespace opencensus {
namespace exporters {
namespace trace {

class ZipkinExporterTestPeer : public ::testing::Test {
 public:
  ZipkinExporterTestPeer() {
    // Register zipkin exporter
    ZipkinExporterOptions options("http://127.0.0.1:9411/api/v2/spans");
    options.service_name = "TestService";
    ZipkinExporter::Register(options);
  }
};

TEST_F(ZipkinExporterTestPeer, ExportTrace) {
  ::opencensus::trace::AlwaysSampler sampler;
  ::opencensus::trace::StartSpanOptions opts = {&sampler};

  auto span1 = ::opencensus::trace::Span::StartSpan("Span1", nullptr, opts);
  absl::SleepFor(absl::Milliseconds(100));
  span1.AddAnnotation("Annotation1", {{"TestBool", true}});
  auto span2 = ::opencensus::trace::Span::StartSpan("Span2", &span1, opts);
  absl::SleepFor(absl::Milliseconds(100));
  span2.AddAnnotation("Annotation2",
                      {{"TestString", "Test"}, {"TestInt", 123}});
  auto span3 = ::opencensus::trace::Span::StartSpan("Span3", &span2, opts);
  span3.AddAttributes({{"key1", "value1"},
                       {"int_key", 123},
                       {"another_key", "another_value"},
                       {"bool_key", true}});
  span3.AddAnnotation("Annotation3", {{"TestString", "Test"}});
  span3.AddSentMessageEvent(2, 3, 4);
  span3.AddReceivedMessageEvent(3, 4, 5);
  absl::SleepFor(absl::Milliseconds(100));
  span3.End();
  absl::SleepFor(absl::Milliseconds(100));
  span2.End();
  absl::SleepFor(absl::Milliseconds(100));
  span1.End();

  // Wait long enough for spans to be exporter to zipkin server.
  absl::SleepFor(absl::Seconds(6));
}

}  // namespace trace
}  // namespace exporters
}  // namespace opencensus
