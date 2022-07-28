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

#include "opencensus/exporters/stats/stdout/stdout_exporter.h"

#include <iostream>
#include <sstream>

#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "opencensus/stats/stats.h"
#include "opencensus/stats/stats_exporter.h"
#include "opencensus/stats/testing/test_utils.h"

namespace opencensus {
namespace stats {

class StatsExporterTest {
 public:
  static constexpr auto& ExportForTesting = StatsExporter::ExportForTesting;
};

}  // namespace stats
}  // namespace opencensus

namespace {

using ::testing::HasSubstr;

TEST(StdoutExporterTest, Export) {
  std::stringstream s;
  ::opencensus::exporters::stats::StdoutExporter::Register(&s);

  auto key = ::opencensus::tags::TagKey::Register("test_key");
  auto measure = ::opencensus::stats::MeasureDouble::Register(
      "example.com/Test/Measure", "Test description.", "1");
  auto descriptor = ::opencensus::stats::ViewDescriptor()
                        .set_name("example.com/Test/View")
                        .set_measure("example.com/Test/Measure")
                        .set_aggregation(opencensus::stats::Aggregation::Sum())
                        .add_column(key);
  descriptor.RegisterForExport();
  ::opencensus::stats::Record({{measure, 123456.}}, {{key, "value1"}});

  ::opencensus::stats::testing::TestUtils::Flush();
  ::opencensus::stats::StatsExporterTest::ExportForTesting();

  const std::string str = s.str();
  std::cout << str;
  EXPECT_THAT(str, HasSubstr("test_key"));
  EXPECT_THAT(str, HasSubstr("value1"));
  EXPECT_THAT(str, HasSubstr("123456"));
}

}  // namespace
