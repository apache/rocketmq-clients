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

#include "opencensus/exporters/trace/stdout/stdout_exporter.h"

#include <iostream>
#include <sstream>

#include "absl/time/clock.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "opencensus/trace/exporter/span_exporter.h"
#include "opencensus/trace/internal/local_span_store.h"
#include "opencensus/trace/sampler.h"
#include "opencensus/trace/span.h"

namespace opencensus {
namespace trace {
namespace exporter {

class SpanExporterTestPeer {
 public:
  static constexpr auto& ExportForTesting = SpanExporter::ExportForTesting;
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

namespace {

using ::testing::HasSubstr;

TEST(StdoutExporterTest, Export) {
  std::stringstream s;
  ::opencensus::exporters::trace::StdoutExporter::Register(&s);
  static ::opencensus::trace::AlwaysSampler sampler;
  ::opencensus::trace::StartSpanOptions opts = {&sampler};

  auto span1 = ::opencensus::trace::Span::StartSpan("Span1", nullptr, opts);
  auto span2 = ::opencensus::trace::Span::StartSpan("Span2", &span1, opts);
  auto span3 = ::opencensus::trace::Span::StartSpan("Span3", &span2, opts);
  span3.AddAnnotation("Needle.");
  span3.End();
  span2.End();
  span1.End();

  opencensus::trace::exporter::SpanExporterTestPeer::ExportForTesting();
  const std::string str = s.str();
  std::cout << str;
  EXPECT_THAT(str, HasSubstr("Needle."));
}

}  // namespace
