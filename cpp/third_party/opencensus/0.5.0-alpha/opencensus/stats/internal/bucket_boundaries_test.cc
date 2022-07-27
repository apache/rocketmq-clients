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

#include "opencensus/stats/bucket_boundaries.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace opencensus {
namespace stats {
namespace {

TEST(BucketBoundariesTest, Linear) {
  const int num_finite_buckets = 3;
  const double width = 1.5;
  const double offset = 2;
  const BucketBoundaries bucket_boundaries =
      BucketBoundaries::Linear(num_finite_buckets, offset, width);
  EXPECT_EQ(num_finite_buckets + 2, bucket_boundaries.num_buckets());
  EXPECT_THAT(bucket_boundaries.lower_boundaries(),
              ::testing::ElementsAre(2, 3.5, 5, 6.5));
}

TEST(BucketBoundariesTest, Exponential) {
  const int num_finite_buckets = 3;
  const double growth_factor = 2;
  const double scale = 1.5;
  const BucketBoundaries bucket_boundaries =
      BucketBoundaries::Exponential(num_finite_buckets, scale, growth_factor);
  EXPECT_EQ(num_finite_buckets + 2, bucket_boundaries.num_buckets());
  EXPECT_THAT(bucket_boundaries.lower_boundaries(),
              ::testing::ElementsAre(0, 1.5, 3, 6));
}

TEST(BucketBoundariesTest, Explicit) {
  const std::initializer_list<double> boundaries = {0, 2, 5, 10};
  const BucketBoundaries bucket_boundaries =
      BucketBoundaries::Explicit(boundaries);
  EXPECT_EQ(boundaries.size() + 1, bucket_boundaries.num_buckets());
  EXPECT_THAT(boundaries, ::testing::ElementsAreArray(
                              bucket_boundaries.lower_boundaries()));
}

TEST(BucketBoundariesTest, BucketForValue) {
  BucketBoundaries bucket_boundaries = BucketBoundaries::Explicit({0, 10});
  EXPECT_EQ(0, bucket_boundaries.BucketForValue(-1));
  EXPECT_EQ(1, bucket_boundaries.BucketForValue(0));
  EXPECT_EQ(1, bucket_boundaries.BucketForValue(1));
  EXPECT_EQ(2, bucket_boundaries.BucketForValue(10));
  EXPECT_EQ(2, bucket_boundaries.BucketForValue(11));
}

TEST(BucketBoundariesTest, BucketForValueEmptyBuckets) {
  BucketBoundaries bucket_boundaries = BucketBoundaries::Explicit({});
  EXPECT_EQ(0, bucket_boundaries.BucketForValue(-1000));
  EXPECT_EQ(0, bucket_boundaries.BucketForValue(0));
  EXPECT_EQ(0, bucket_boundaries.BucketForValue(1000));
}

TEST(BucketBoundariesDeathTest, NonMonotonicExplicit) {
  const std::initializer_list<double> boundaries = {0, -1, 1};
  EXPECT_DEBUG_DEATH(
      {
        EXPECT_TRUE(
            BucketBoundaries::Explicit(boundaries).lower_boundaries().empty());
      },
      "");
}

}  // namespace
}  // namespace stats
}  // namespace opencensus
