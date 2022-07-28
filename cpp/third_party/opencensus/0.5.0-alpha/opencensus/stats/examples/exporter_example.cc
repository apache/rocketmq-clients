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

#include <iostream>

#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gtest/gtest.h"
#include "opencensus/stats/stats.h"

namespace opencensus_examples {
namespace {

// The resource owner publicly defines their measure IDs.
constexpr char kFooUsageMeasureName[] = "example.com/Foo/FooUsage";

opencensus::stats::MeasureDouble FooUsage() {
  static const opencensus::stats::MeasureDouble foo_usage =
      opencensus::stats::MeasureDouble::Register(kFooUsageMeasureName, "foos",
                                                 "Usage of foos.");
  return foo_usage;
}

opencensus::tags::TagKey FooIdTagKey() {
  static const auto foo_id_tag_key =
      opencensus::tags::TagKey::Register("foo_id");
  return foo_id_tag_key;
}

// An example exporter that exports to stdout.
class ExampleExporter : public opencensus::stats::StatsExporter::Handler {
 public:
  static void Register() {
    opencensus::stats::StatsExporter::RegisterPushHandler(
        absl::make_unique<ExampleExporter>());
  }

  void ExportViewData(
      const std::vector<std::pair<opencensus::stats::ViewDescriptor,
                                  opencensus::stats::ViewData>>& data)
      override {
    for (const auto& datum : data) {
      const auto& descriptor = datum.first;
      const auto& view_data = datum.second;
      if (view_data.type() != opencensus::stats::ViewData::Type::kDouble) {
        // This example only supports double data (i.e. Sum() aggregation).
        return;
      }
      std::string output;
      absl::StrAppend(&output, "\nData for view \"", descriptor.name(), "\n");
      for (const auto& row : view_data.double_data()) {
        auto start_time = view_data.start_times().at(row.first);
        absl::StrAppend(&output, "\nRow data from ",
                        absl::FormatTime(start_time), " to ",
                        absl::FormatTime(view_data.end_time()), ":\n");
        for (int i = 0; i < descriptor.columns().size(); ++i) {
          absl::StrAppend(&output, descriptor.columns()[i].name(), ":",
                          row.first[i], ", ");
        }
        absl::StrAppend(&output, row.second, "\n");
      }
      std::cout << output;
    }
  }
};

class ExporterExample : public ::testing::Test {
 protected:
  void SetupFoo() { FooUsage(); }

  void UseFoo(absl::string_view id, double quantity) {
    opencensus::stats::Record({{FooUsage(), quantity}}, {{FooIdTagKey(), id}});
  }
};

TEST_F(ExporterExample, Distribution) {
  // Measure initialization must precede view creation.
  SetupFoo();

  // The stats consumer creates a View on Foo Usage.
  const auto sum_descriptor =
      opencensus::stats::ViewDescriptor()
          .set_name("example.com/Bar/FooUsage-sum-cumulative-foo_id")
          .set_measure(kFooUsageMeasureName)
          .set_aggregation(opencensus::stats::Aggregation::Sum())
          .add_column(FooIdTagKey())
          .set_description(
              "Cumulative sum of example.com/Foo/FooUsage broken down "
              "by 'foo_id'.");
  const auto count_descriptor =
      opencensus::stats::ViewDescriptor()
          .set_name("example.com/Bar/FooUsage-sum-interval-foo_id")
          .set_measure(kFooUsageMeasureName)
          .set_aggregation(opencensus::stats::Aggregation::Count())
          .add_column(FooIdTagKey())
          .set_description(
              "Cumulative count of example.com/Foo/FooUsage broken down by "
              "'foo_id'.");

  // The order of view registration and exporter creation does not matter, as
  // long as both precede data recording.
  sum_descriptor.RegisterForExport();
  ExampleExporter::Register();
  count_descriptor.RegisterForExport();

  // Someone calls the Foo API, recording usage under example.com/Bar/FooUsage.
  UseFoo("foo1", 1);
  UseFoo("foo1", 4);
  UseFoo("foo2", 2);
  // Stats are exported every 10 seconds.
  absl::SleepFor(absl::Seconds(11));

  UseFoo("foo1", 1);
  UseFoo("foo2", 5);
  absl::SleepFor(absl::Seconds(11));
}

}  // namespace
}  // namespace opencensus_examples
