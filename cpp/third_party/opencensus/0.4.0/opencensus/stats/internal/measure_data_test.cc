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

#include "opencensus/stats/internal/measure_data.h"

#include <algorithm>
#include <cmath>
#include <numeric>
#include <vector>

#include "absl/types/span.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "opencensus/stats/bucket_boundaries.h"
#include "opencensus/stats/distribution.h"
#include "opencensus/stats/testing/test_utils.h"

namespace opencensus {
namespace stats {
namespace {

TEST(MeasureDataTest, SmallSequence) {
  MeasureData data({});

  data.Add(-6);
  data.Add(0);
  data.Add(3);

  EXPECT_EQ(data.count(), 3);
  EXPECT_DOUBLE_EQ(data.sum(), -3);
}

TEST(MeasureDataTest, MultipleHistograms) {
  std::vector<BucketBoundaries> buckets = {BucketBoundaries::Explicit({0, 10}),
                                           BucketBoundaries::Explicit({}),
                                           BucketBoundaries::Explicit({5})};
  MeasureData data(buckets);
  data.Add(-1);
  data.Add(1);
  data.Add(8);

  Distribution distribution1 =
      testing::TestUtils::MakeDistribution(&buckets[0]);
  data.AddToDistribution(&distribution1);
  EXPECT_THAT(distribution1.bucket_counts(), ::testing::ElementsAre(1, 2, 0));

  Distribution distribution2 =
      testing::TestUtils::MakeDistribution(&buckets[2]);
  data.AddToDistribution(&distribution2);
  EXPECT_THAT(distribution2.bucket_counts(), ::testing::ElementsAre(2, 1));
}

TEST(MeasureDataTest, DistributionStatistics) {
  BucketBoundaries buckets = BucketBoundaries::Explicit({});
  MeasureData data(absl::MakeSpan(&buckets, 1));

  const std::vector<int> samples{91, 18, 63, 98, 87, 77, 14, 97, 10, 35,
                                 12, 5,  75, 41, 49, 38, 40, 20, 55, 83};
  const double expected_mean =
      static_cast<double>(std::accumulate(samples.begin(), samples.end(), 0)) /
      samples.size();
  double expected_sum_of_squared_deviation = 0;
  for (const auto sample : samples) {
    data.Add(sample);
    expected_sum_of_squared_deviation += pow(sample - expected_mean, 2);
  }

  Distribution distribution = testing::TestUtils::MakeDistribution(&buckets);
  data.AddToDistribution(&distribution);
  EXPECT_EQ(distribution.count(), samples.size());
  EXPECT_DOUBLE_EQ(distribution.mean(), expected_mean);
  EXPECT_DOUBLE_EQ(distribution.sum_of_squared_deviation(),
                   expected_sum_of_squared_deviation);
  EXPECT_DOUBLE_EQ(distribution.min(),
                   *std::min_element(samples.begin(), samples.end()));
  EXPECT_DOUBLE_EQ(distribution.max(),
                   *std::max_element(samples.begin(), samples.end()));
}

TEST(MeasureDataTest, BatchedAddToDistribution) {
  // Tests that batching values in the MeasureData is equivalent to sequentially
  // adding to the distribution.
  BucketBoundaries buckets = BucketBoundaries::Exponential(7, 2, 2);
  MeasureData data(absl::MakeSpan(&buckets, 1));
  Distribution base_distribution =
      testing::TestUtils::MakeDistribution(&buckets);
  // Add some preexisting data to fully test the merge.
  testing::TestUtils::AddToDistribution(&base_distribution, 20);
  testing::TestUtils::AddToDistribution(&base_distribution, 10);

  Distribution expected_distribution = base_distribution;

  const double tolerance = 1.0 / 1000000000;
  const int max = 100;
  for (int i = 0; i <= max; ++i) {
    data.Add(i);
    testing::TestUtils::AddToDistribution(&expected_distribution, i);

    Distribution actual_distribution = base_distribution;
    data.AddToDistribution(&actual_distribution);

    EXPECT_EQ(expected_distribution.count(), actual_distribution.count());
    EXPECT_DOUBLE_EQ(expected_distribution.mean(), actual_distribution.mean());
    EXPECT_NEAR(expected_distribution.sum_of_squared_deviation(),
                actual_distribution.sum_of_squared_deviation(), tolerance);
    EXPECT_DOUBLE_EQ(expected_distribution.min(), actual_distribution.min());
    EXPECT_DOUBLE_EQ(expected_distribution.max(), actual_distribution.max());
    EXPECT_THAT(
        actual_distribution.bucket_counts(),
        ::testing::ElementsAreArray(expected_distribution.bucket_counts()));
  }
}

TEST(MeasureDataDeathTest, AddToDistributionWithUnknownBuckets) {
  BucketBoundaries buckets = BucketBoundaries::Explicit({0, 10});
  MeasureData data(absl::MakeSpan(&buckets, 1));
  data.Add(1);

  BucketBoundaries distribution_buckets = BucketBoundaries::Explicit({0});
  Distribution distribution =
      testing::TestUtils::MakeDistribution(&distribution_buckets);
  EXPECT_DEBUG_DEATH(
      {
        data.AddToDistribution(&distribution);
        EXPECT_THAT(distribution.bucket_counts(), ::testing::ElementsAre(1, 0));
      },
      "No matching BucketBoundaries in AddToDistribution");
}

}  // namespace
}  // namespace stats
}  // namespace opencensus
