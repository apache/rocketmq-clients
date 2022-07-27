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

#include "opencensus/stats/stats_exporter.h"

#include <cstdint>
#include <utility>
#include <vector>

#include "absl/memory/memory.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "opencensus/stats/internal/set_aggregation_window.h"
#include "opencensus/stats/measure.h"
#include "opencensus/stats/measure_descriptor.h"
#include "opencensus/stats/view_descriptor.h"

namespace opencensus {
namespace stats {

// A mock exporter that assigns exported data to the provided pointer.
class MockExporter : public StatsExporter::Handler {
 public:
  static void Register(
      std::vector<std::pair<ViewDescriptor, ViewData>>* output) {
    opencensus::stats::StatsExporter::RegisterPushHandler(
        absl::make_unique<MockExporter>(output));
  }

  MockExporter(std::vector<std::pair<ViewDescriptor, ViewData>>* output)
      : output_(output) {}

  void ExportViewData(
      const std::vector<std::pair<ViewDescriptor, ViewData>>& data) override {
    // Looping because ViewData is (intentionally) copy-constructable but not
    // copy_assignable.
    for (const auto& datum : data) {
      output_->emplace_back(datum.first, datum.second);
    }
  }

 private:
  std::vector<std::pair<ViewDescriptor, ViewData>>* output_;
};

constexpr char kMeasureId[] = "test_measure_id";

MeasureDouble TestMeasure() {
  static MeasureDouble measure = MeasureDouble::Register(kMeasureId, "", "1");
  return measure;
}

class StatsExporterTest : public ::testing::Test {
 protected:
  void SetUp() {
    // Access the measure to ensure it has been registered.
    TestMeasure();
    descriptor1_.set_name("id1");
    descriptor1_.set_measure(kMeasureId);
    descriptor1_.set_aggregation(Aggregation::Count());
    descriptor1_edited_.set_name("id1");
    descriptor1_edited_.set_measure(kMeasureId);
    descriptor1_edited_.set_aggregation(Aggregation::Sum());
    descriptor2_.set_name("id2");
    descriptor2_.set_measure(kMeasureId);
    descriptor2_.set_aggregation(
        Aggregation::Distribution(BucketBoundaries::Explicit({0})));
  }

  void TearDown() {
    StatsExporter::RemoveView(descriptor1_.name());
    StatsExporter::RemoveView(descriptor2_.name());
    StatsExporter::ClearHandlersForTesting();
  }

  static void Export() { StatsExporter::ExportForTesting(); }

  ViewDescriptor descriptor1_;
  ViewDescriptor descriptor1_edited_;
  ViewDescriptor descriptor2_;
};

TEST_F(StatsExporterTest, AddView) {
  std::vector<std::pair<ViewDescriptor, ViewData>> exported_data;
  MockExporter::Register(&exported_data);
  descriptor1_.RegisterForExport();
  descriptor2_.RegisterForExport();
  EXPECT_THAT(StatsExporter::GetViewData(),
              ::testing::UnorderedElementsAre(::testing::Key(descriptor1_),
                                              ::testing::Key(descriptor2_)));
  Export();
  EXPECT_THAT(exported_data,
              ::testing::UnorderedElementsAre(::testing::Key(descriptor1_),
                                              ::testing::Key(descriptor2_)));
}

TEST_F(StatsExporterTest, UpdateView) {
  std::vector<std::pair<ViewDescriptor, ViewData>> exported_data;
  MockExporter::Register(&exported_data);
  descriptor1_.RegisterForExport();
  descriptor2_.RegisterForExport();
  descriptor1_edited_.RegisterForExport();
  EXPECT_THAT(
      StatsExporter::GetViewData(),
      ::testing::UnorderedElementsAre(::testing::Key(descriptor1_edited_),
                                      ::testing::Key(descriptor2_)));
  Export();
  EXPECT_THAT(exported_data, ::testing::UnorderedElementsAre(
                                 ::testing::Key(descriptor1_edited_),
                                 ::testing::Key(descriptor2_)));
}

TEST_F(StatsExporterTest, RemoveView) {
  std::vector<std::pair<ViewDescriptor, ViewData>> exported_data;
  MockExporter::Register(&exported_data);
  descriptor1_.RegisterForExport();
  descriptor2_.RegisterForExport();
  StatsExporter::RemoveView(descriptor1_.name());
  EXPECT_THAT(StatsExporter::GetViewData(),
              ::testing::UnorderedElementsAre(::testing::Key(descriptor2_)));
  Export();
  EXPECT_THAT(exported_data,
              ::testing::UnorderedElementsAre(::testing::Key(descriptor2_)));
}

TEST_F(StatsExporterTest, MultipleExporters) {
  std::vector<std::pair<ViewDescriptor, ViewData>> exported_data_1;
  MockExporter::Register(&exported_data_1);
  std::vector<std::pair<ViewDescriptor, ViewData>> exported_data_2;
  MockExporter::Register(&exported_data_2);
  descriptor1_.RegisterForExport();
  Export();
  EXPECT_THAT(exported_data_1,
              ::testing::UnorderedElementsAre(::testing::Key(descriptor1_)));
  EXPECT_THAT(exported_data_2,
              ::testing::UnorderedElementsAre(::testing::Key(descriptor1_)));
}

TEST_F(StatsExporterTest, IntervalViewRejected) {
  std::vector<std::pair<ViewDescriptor, ViewData>> exported_data;
  MockExporter::Register(&exported_data);
  ViewDescriptor interval_descriptor = ViewDescriptor().set_name("interval");
  SetAggregationWindow(AggregationWindow::Interval(absl::Hours(1)),
                       &interval_descriptor);
  interval_descriptor.RegisterForExport();
  EXPECT_TRUE(StatsExporter::GetViewData().empty());
  Export();
  EXPECT_TRUE(exported_data.empty());
}

TEST_F(StatsExporterTest, TimedExport) {
  std::vector<std::pair<ViewDescriptor, ViewData>> exported_data;
  MockExporter::Register(&exported_data);
  descriptor1_.RegisterForExport();
  absl::SleepFor(absl::Seconds(11));
  EXPECT_THAT(exported_data,
              ::testing::UnorderedElementsAre(::testing::Key(descriptor1_)));
}

}  // namespace stats
}  // namespace opencensus
