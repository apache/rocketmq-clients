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

#include <string>

#include "absl/time/time.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "opencensus/stats/aggregation.h"
#include "opencensus/stats/bucket_boundaries.h"
#include "opencensus/stats/internal/aggregation_window.h"
#include "opencensus/stats/measure.h"
#include "opencensus/stats/measure_descriptor.h"
#include "opencensus/stats/view_descriptor.h"

namespace opencensus {
namespace stats {
namespace {

TEST(DebugStringTest, Aggregation) {
  EXPECT_NE("", Aggregation::Count().DebugString());
  EXPECT_NE("", Aggregation::Sum().DebugString());
  EXPECT_NE("", Aggregation::LastValue().DebugString());

  const BucketBoundaries buckets = BucketBoundaries::Explicit({0, 1});
  EXPECT_PRED_FORMAT2(::testing::IsSubstring, buckets.DebugString(),
                      Aggregation::Distribution(buckets).DebugString());
}

TEST(DebugStringTest, AggregationWindow) {
  EXPECT_NE("", AggregationWindow::Cumulative().DebugString());
  EXPECT_NE("", AggregationWindow::Delta().DebugString());
  EXPECT_NE("", AggregationWindow::Interval(absl::Minutes(1)).DebugString());
}

TEST(DebugStringTest, MeasureDescriptor) {
  const std::string name = "foo";
  const std::string description = "Usage of foo";
  const std::string units = "1";
  static const MeasureDescriptor descriptor =
      MeasureInt64::Register(name, description, units).GetDescriptor();
  EXPECT_PRED_FORMAT2(::testing::IsSubstring, name, descriptor.DebugString());
  EXPECT_PRED_FORMAT2(::testing::IsSubstring, units, descriptor.DebugString());
  EXPECT_PRED_FORMAT2(::testing::IsSubstring, description,
                      descriptor.DebugString());
}

TEST(DebugStringTest, ViewDescriptor) {
  const Aggregation aggregation = Aggregation::Count();
  const AggregationWindow aggregation_window =
      AggregationWindow::Interval(absl::Minutes(1));
  const std::string measure_name = "bar";
  static const auto measure = MeasureDouble::Register(measure_name, "", "");
  static const opencensus::tags::TagKey tag_key =
      opencensus::tags::TagKey::Register("tag_key_1");
  MeasureDouble::Register(measure_name, "", "");
  const std::string description = "description string";
  ViewDescriptor descriptor = ViewDescriptor()
                                  .set_measure(measure_name)
                                  .set_aggregation(aggregation)
                                  .add_column(tag_key)
                                  .set_description(description);
  SetAggregationWindow(aggregation_window, &descriptor);

  EXPECT_PRED_FORMAT2(::testing::IsSubstring,
                      measure.GetDescriptor().DebugString(),
                      descriptor.DebugString());
  EXPECT_PRED_FORMAT2(::testing::IsSubstring, aggregation.DebugString(),
                      descriptor.DebugString());
  EXPECT_PRED_FORMAT2(::testing::IsSubstring, aggregation_window.DebugString(),
                      descriptor.DebugString());
  EXPECT_PRED_FORMAT2(::testing::IsSubstring, tag_key.name(),
                      descriptor.DebugString());
  EXPECT_PRED_FORMAT2(::testing::IsSubstring, description,
                      descriptor.DebugString());
}

}  // namespace
}  // namespace stats
}  // namespace opencensus
