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

#include "opencensus/common/internal/stats_object.h"

#include <cmath>
#include <cstdlib>
#include <deque>
#include <iostream>
#include <numeric>

#include "absl/strings/str_join.h"
#include "absl/types/span.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace opencensus {
namespace common {
namespace {

// We use epsilon to avoid boundary conditions, since exactly what we do at the
// interval boundaries is implementation-defined.
constexpr absl::Duration epsilon = absl::Nanoseconds(10);

template <class StatsObjectType>
void CheckSum(const StatsObjectType& obj, absl::Time now,
              const std::vector<double>& expected_sum) {
  std::vector<double> sum = obj.Sum(now);
  bool success = expected_sum.size() == sum.size();
  if (success) {
    for (int i = 0; i < expected_sum.size(); ++i) {
      // Close enough is close enough.
      if (std::abs(expected_sum[i] - sum[i]) >
          100 * absl::ToDoubleSeconds(epsilon)) {
        success = false;
        break;
      }
    }
  }
  EXPECT_TRUE(success) << "Got {" << absl::StrJoin(sum, ",")
                       << "} but expected something near {"
                       << absl::StrJoin(expected_sum, ",") << "}";
}

TEST(StatsObjectTest, InitiallyEmpty) {
  StatsObject<4> obj(2, absl::Minutes(1), absl::UnixEpoch());
  CheckSum(obj, absl::UnixEpoch(), {0, 0});
}

TEST(StatsObjectTest, FirstBucketOnly) {
  StatsObject<4> obj(1, absl::Minutes(1), absl::UnixEpoch());
  absl::Span<double> b = obj.MutableCurrentBucket(absl::UnixEpoch());
  EXPECT_EQ(1, b.size());
  EXPECT_EQ(0, b[0]);
  b[0]++;
  CheckSum(obj, absl::UnixEpoch(), {1});
}

TEST(StatsObjectTest, TwoBuckets) {
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj(1, absl::Minutes(1), t0);
  absl::Span<double> b = obj.MutableCurrentBucket(t0);
  b[0]++;

  // t2 falls into the second bucket.
  const absl::Time t2 = t0 + obj.bucket_interval() + epsilon;
  b = obj.MutableCurrentBucket(t2);
  EXPECT_EQ(1, b.size());
  EXPECT_EQ(0, b[0]);
  b[0] += 2;

  CheckSum(obj, t2, {3});
}

TEST(StatsObjectTest, BucketBoundary) {
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj(1, absl::Minutes(1), t0);
  obj.Add({1}, t0);

  // There shouldn't be a discontinuity between right before the end of the
  // bucket and the end of the bucket.
  EXPECT_NEAR(
      0,
      obj.Sum(t0 + obj.total_interval() + obj.bucket_interval() - epsilon)[0],
      absl::ToDoubleSeconds(epsilon));
  EXPECT_EQ(std::vector<double>(1),
            obj.Sum(t0 + obj.total_interval() + obj.bucket_interval()));
}

TEST(StatsObjectTest, GetStatsFuture) {
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj(1, absl::Minutes(1), t0);
  int num_buckets = obj.total_interval() / obj.bucket_interval();
  for (int i = 0; i < num_buckets; ++i) {
    obj.MutableCurrentBucket(t0 + i * obj.bucket_interval() + epsilon)[0] =
        i + 1;
  }
  // Now get the sum at points increasingly far into the future.
  double total_added = num_buckets * (num_buckets + 1) / 2;
  for (int i = 0; i < 3 * num_buckets; ++i) {
    double total_dropped = (i - 1) * i / 2;
    CheckSum(obj, t0 + (i + num_buckets - 1) * obj.bucket_interval() + epsilon,
             {std::max(0.0, total_added - total_dropped)});
    // Our interpolation scheme should ensure that we get the same answer for
    // now +/- epsilon.  But this doesn't work when i == 0, because calling
    // MutableCurrentBucket(t1) and then GetSum(t2) with t2 < t1 isn't allowed.
    if (i != 0) {
      CheckSum(obj,
               t0 + (i + num_buckets - 1) * obj.bucket_interval() - epsilon,
               {std::max(0.0, total_added - total_dropped)});
    }
  }
}

TEST(StatsObjectTest, IncompleteFirstBucket) {
  // Create this object 1/3 of the way into its first bucket's interval.
  const absl::Duration interval =
      StatsObject<4>(1, absl::Minutes(1), absl::UnixEpoch()).bucket_interval();
  const absl::Time t0 = absl::UnixEpoch() + interval / 3;
  StatsObject<4> obj(1, absl::Minutes(1), t0);
  int num_buckets = obj.total_interval() / interval;
  for (int i = 0; i < num_buckets; ++i) {
    obj.MutableCurrentBucket(t0 + i * interval)[0] = i + 1;
  }
  absl::Time time = absl::UnixEpoch() + num_buckets * interval + epsilon;
  obj.MutableCurrentBucket(time)[0] = num_buckets + 1;

  double total = (num_buckets + 1) * (num_buckets + 2) / 2.0;
  // Here time is just barely into the beginning new bucket.  We include
  // everything we can in this case, so we take all of the first bucket.
  CheckSum(obj, time, {total});
  // Here time is 1/4 of the way into the new bucket.  The first bucket was
  // created 1/3 of the way into its interval, so covers 2/3 of its interval.
  // Since 1/4 + 2/3 <= 1, we include all of the first bucket.
  CheckSum(obj, time + 1 / 4 * interval, {total});
  // Same as previous check: Since 1/3 + 2/3 <= 1, we include all of the first
  // bucket.
  CheckSum(obj, time + 1 / 3 * interval, {total});

  // The first bucket's value is 1.
  double total_without_first = total - 1;
  // Here time is 2/3 of the way into the new bucket.  Thus we would use 1 - 2/3
  // = 1/3 of the first bucket, if it covered a full interval.  But the first
  // bucket's data only covers 2/3 of an interval, so we actually include
  // (1/3) / (2/3) = 1/2 of the first bucket.
  CheckSum(obj, time + 2.0 / 3 * interval, {total_without_first + 1.0 / 2});
  // Here time is 14/15 of the way into the new bucket.  Thus we would use 1/15
  // of the first bucket, if it covered a full interval.  But the first bucket's
  // data only covers 2/3 of an interval, so we actually include
  // (1/15) / (2/3) = 1/10 of the first bucket.
  CheckSum(obj, time + 14.0 / 15 * interval, {total_without_first + 1.0 / 10});

  // Now push another bucket into the object.  At this point, the initial bucket
  // has been pushed off the queue, so our math is simpler this time.
  time += interval;
  obj.MutableCurrentBucket(time)[0] = num_buckets + 2;
  total = total_without_first + num_buckets + 2;
  CheckSum(obj, time, {total});
  CheckSum(obj, time + 1.0 / 3 * interval, {total - 1.0 / 3 * 2});
  CheckSum(obj, time + 2.0 / 3 * interval, {total - 2.0 / 3 * 2});
  CheckSum(obj, time + 14.0 / 15 * interval, {total - 14.0 / 15.0 * 2});
}

TEST(StatsObjectTest, Shift) {
  const int num_elems = 3;
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj(num_elems, absl::Minutes(1), t0);
  const int num_buckets = obj.total_interval() / obj.bucket_interval();
  const absl::Duration interval = obj.bucket_interval();
  std::vector<double> sum(num_elems);
  int i;
  for (i = 0; i < num_buckets; ++i) {
    const absl::Time time = t0 + (i + 1) * interval - epsilon;
    absl::Span<double> bucket = obj.MutableCurrentBucket(time);
    EXPECT_EQ(num_elems, bucket.size());
    for (int j = 0; j < bucket.size(); ++j) {
      EXPECT_EQ(0, bucket[j]) << "i=" << i << ", j=" << j;
      double v = i * bucket.size() + j;
      bucket[j] += v;
      sum[j] += v;
    }
    CheckSum(obj, time, sum);
  }
  for (; i < 20; ++i) {
    for (int j = 0; j < num_elems; ++j) {
      sum[j] -= (i - num_buckets) * num_elems + j;
    }
    const absl::Time time = t0 + (i + 1) * interval - epsilon;
    absl::Span<double> bucket = obj.MutableCurrentBucket(time);
    EXPECT_EQ(num_elems, bucket.size());
    for (int j = 0; j < bucket.size(); ++j) {
      double v = i * bucket.size() + j;
      bucket[j] += v;
      sum[j] += v;
    }
    CheckSum(obj, time, sum);
  }
}

TEST(StatsObjectTest, Add) {
  const int num_elems = 3;
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj(num_elems, absl::Hours(1), t0);
  const int num_buckets = obj.total_interval() / obj.bucket_interval();
  const absl::Duration interval = obj.bucket_interval();
  std::vector<double> sum(num_elems);
  for (int i = 0; i < num_buckets; ++i) {
    if (i >= num_buckets) {
      for (int j = 0; j < num_elems; ++j) {
        sum[j] -= (i - num_buckets) * num_elems + j;
      }
    }
    std::vector<double> vals_to_add(num_elems);
    for (int j = 0; j < num_elems; ++j) {
      double v = i * num_elems + j;
      vals_to_add[j] += v;
      sum[j] += v;
    }
    const absl::Time time = t0 + (i + 1) * interval - epsilon;
    obj.Add(vals_to_add, time);
    CheckSum(obj, time, sum);
  }
}

// Merge obj2 into obj1, where obj2 is much older than obj1.
TEST(StatsObjectTest, MergeVeryOld) {
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj1(1, absl::Hours(1), t0);
  StatsObject<4> obj2(1, absl::Hours(1), t0);
  const absl::Time obj1_time =
      t0 + obj1.total_interval() + obj1.bucket_interval() + epsilon;
  obj1.Add({1}, obj1_time);
  obj2.Add({2}, t0);
  obj1.Merge(obj2);
  CheckSum(obj1, obj1_time, {1});
}

// Merge obj2 into obj1, where obj2 is much newer than obj1.
TEST(StatsObjectTest, MergeVeryNew) {
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj1(1, absl::Hours(1), t0);
  StatsObject<4> obj2(1, absl::Hours(1), t0);
  const absl::Time obj2_time =
      t0 + obj2.total_interval() + obj2.bucket_interval() + epsilon;
  obj1.Add({1}, t0);
  obj2.Add({2}, obj2_time);
  obj1.Merge(obj2);
  CheckSum(obj1, obj2_time, {2});
}

TEST(StatsObjectTest, MergeOffset) {
  const absl::Duration interval =
      StatsObject<4>(1, absl::Hours(1), absl::UnixEpoch()).bucket_interval();
  const int num_buckets =
      StatsObject<4>(1, absl::Hours(1), absl::UnixEpoch()).total_interval() /
      interval;

  // All our times are relative to t0; this lets us avoid negative time.
  const absl::Time t0 = absl::UnixEpoch() + 1000 * interval;
  for (int i = -10; i < 11; ++i) {
    StatsObject<4> obj1(1, absl::Hours(1), absl::UnixEpoch());
    StatsObject<4> obj2(1, absl::Hours(1), absl::UnixEpoch());
    std::deque<double> elems1(num_buckets + 1);
    std::deque<double> elems2(num_buckets + 1);
    absl::Time time1;
    absl::Time time2;
    for (int j = 0; j < num_buckets + 1; ++j) {
      elems1.push_front(j + 1);
      if (elems1.size() > num_buckets + 1) {
        elems1.pop_back();
      }
      time1 = t0 + interval * j + epsilon;
      obj1.Add({elems1.front()}, time1);

      elems2.push_front(1000 * (j + 1));
      if (elems2.size() > num_buckets + 1) {
        elems2.pop_back();
      }
      time2 = t0 + interval * (i + j) + epsilon;
      obj2.Add({elems2.front()}, time2);
    }
    CheckSum(obj1, time1, {std::accumulate(elems1.begin(), elems1.end(), 0.0)});
    CheckSum(obj2, time2, {std::accumulate(elems2.begin(), elems2.end(), 0.0)});

    obj1.Merge(obj2);

    // Pop elements off elems1 or elems2, according to whether obj1 or obj2 is
    // ahead.
    for (int j = 0; j < std::abs(i); ++j) {
      if (i < 0) {
        // obj1 is ahead of obj2; we lose some of obj2's older elements.
        if (!elems2.empty()) {
          elems2.pop_back();
        }
      } else {
        // obj1 is behind obj2; when we merge obj2 into obj1, we'll shift obj1,
        // losing some of its older elements.
        if (!elems1.empty()) {
          elems1.pop_back();
        }
      }
    }
    CheckSum(obj1, std::max(time1, time2),
             {std::accumulate(elems1.begin(), elems1.end(), 0.0) +
              std::accumulate(elems2.begin(), elems2.end(), 0.0)});
  }
}

// Tickle a particular bug which occurs with SimpleStatsObject<4><4, X> when
// merging an object with three buckets filled into an object with four buckets
// filled.
TEST(StatsObjectTest, MergeUnfull) {
  absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj1(2, absl::Minutes(1), t0);
  StatsObject<4> obj2(2, absl::Minutes(1), t0);
  const absl::Duration interval = obj1.bucket_interval();
  obj1.Add({1, 2}, t0 + interval - epsilon);
  obj1.Add({3, 4}, t0 + 2 * interval - epsilon);
  obj1.Add({5, 6}, t0 + 3 * interval - epsilon);
  obj1.Add({7, 8}, t0 + 4 * interval - epsilon);

  obj2.Add({100, 200}, t0 + interval - epsilon);
  obj2.Add({300, 400}, t0 + 2 * interval - epsilon);
  obj2.Add({500, 600}, t0 + 3 * interval - epsilon);

  obj1.Merge(obj2);
  CheckSum(obj1, t0 + 3 * interval - epsilon,
           {1 + 3 + +5 + 7 + 100 + 300 + 500, 2 + 4 + 6 + 8 + 200 + 400 + 600});
}

// Merge objects with different start times.  After doing so, we should consider
// the first bucket's incompleteness to be the smaller of the two objects'
// values.
TEST(StatsObjectTest, MergeIncompleteFirstBucket) {
  const absl::Time t0 = absl::UnixEpoch();
  const absl::Duration interval =
      StatsObject<4>(1, absl::Minutes(1), t0).bucket_interval();
  {
    // obj1's first bucket is 2/3 full; obj2's first bucket is 1/3 full.  After
    // merging, obj2 into obj1, the first bucket should be 2/3 full (i.e., no
    // change).
    StatsObject<4> obj1(1, absl::Minutes(1), t0 + interval / 3.0);
    StatsObject<4> obj2(1, absl::Minutes(1), t0 + 2 * interval / 3.0);
    obj1.Add({1}, t0 + interval / 3.0);
    obj2.Add({10}, t0 + 2 * interval / 3.0);
    obj1.Merge(obj2);
    CheckSum(obj1, t0 + obj1.total_interval() + epsilon, {11});
    CheckSum(obj1, t0 + obj1.total_interval() + interval / 3.0, {11});
    // We're 2/3 of the way into the new interval, so we should take 1/3 of the
    // first bucket.  But the first bucket covers 2/3 of the interval, so we get
    // 1/3 / 2/3 = 1/2 of the first bucket.
    CheckSum(obj1, t0 + obj1.total_interval() + 2 * interval / 3.0, {5.5});
  }

  {
    // Now do the same test, but swap obj1 and obj2's times.  After merging obj2
    // into obj1, the first bucket should again be 2/3 full, up from 1/3 full
    // before the merge.
    StatsObject<4> obj1(1, absl::Minutes(1), t0 + 2 * interval / 3.0);
    StatsObject<4> obj2(1, absl::Minutes(1), t0 + interval / 3.0);
    obj1.Add({1}, t0 + 2 * interval / 3.0);
    obj2.Add({10}, t0 + interval / 3.0);
    obj1.Merge(obj2);
    CheckSum(obj1, t0 + obj1.total_interval() + epsilon, {11});
    CheckSum(obj1, t0 + obj1.total_interval() + interval / 3.0, {11});
    CheckSum(obj1, t0 + obj1.total_interval() + 2 * interval / 3.0, {5.5});
  }
}

TEST(StatsObjectTest, MergeMismatchedNumStats) {
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj1(1, absl::Minutes(1), t0);
  StatsObject<4> obj2(2, absl::Minutes(1), t0);
  obj1.Add({1}, t0 + absl::Seconds(1));
  obj2.Add({2, 2}, t0 + absl::Seconds(1));
  obj1.Merge(obj2);
  CheckSum(obj1, t0 + obj1.total_interval() - epsilon, {1});
  obj2.Merge(obj1);
  CheckSum(obj2, t0 + obj2.total_interval() - epsilon, {2, 2});
}

TEST(StatsObjectTest, MergeMismatchedInterval) {
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj1(1, absl::Minutes(1), t0);
  StatsObject<4> obj2(1, absl::Hours(1), t0);
  obj1.Add({1}, t0 + absl::Seconds(1));
  obj2.Add({2}, t0 + absl::Seconds(1));
  obj1.Merge(obj2);
  CheckSum(obj1, t0 + obj1.total_interval() - epsilon, {1});
  obj2.Merge(obj1);
  CheckSum(obj2, t0 + obj2.total_interval() - epsilon, {2});
}

TEST(StatsObjectTest, Empty) {
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj(2, absl::Minutes(1), t0);
  EXPECT_TRUE(obj.IsEmpty(t0));
  EXPECT_TRUE(obj.IsEmpty(t0 + absl::Seconds(100)));
  // Adding 0 doesn't change whether we're empty.
  obj.Add({0, 0}, t0);
  EXPECT_TRUE(obj.IsEmpty(t0));

  obj.Add({0, 1}, t0);
  EXPECT_FALSE(obj.IsEmpty(t0));
  EXPECT_FALSE(obj.IsEmpty(t0 + obj.total_interval()));
  EXPECT_EQ(std::vector<double>(2),
            obj.Sum(t0 + obj.total_interval() + obj.bucket_interval()));
  EXPECT_TRUE(obj.IsEmpty(t0 + obj.total_interval() + obj.bucket_interval()));
}

TEST(StatsObjectTest, DebugStringDoesntCrash) {
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj(2, absl::Minutes(1), t0);
  obj.DebugString();
  obj.Add({1, 2}, t0);
  std::cout << obj.DebugString() << "\n";
}

TEST(StatsObjectTest, VeryLargeYear) {
  StatsObject<4> obj(1, absl::Minutes(1), absl::UnixEpoch());
  const absl::Time now = absl::FromUnixSeconds(32503680000);  // Year 3000.
  EXPECT_EQ(0, obj.Sum(now)[0]);
  obj.Add({42}, now);
  EXPECT_EQ(42, obj.Sum(now)[0]);
}

TEST(StatsObjectTest, RateEndToEnd) {
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj(2, absl::Minutes(1), t0);
  for (int i = 0; i < 100; ++i) {
    const absl::Time now = t0 + absl::Seconds(i * 10);
    obj.Add({i * 5.0, i * 10.0}, now);
    std::vector<double> sum = obj.Sum(now);
    std::vector<double> rate = obj.Rate(now);
    ASSERT_EQ(2, rate.size());
    EXPECT_DOUBLE_EQ(sum[0] / 60.0, rate[0]);
    EXPECT_DOUBLE_EQ(sum[1] / 60.0, rate[1]);
  }
}

TEST(StatsObjectTest, SumInto) {
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj(2, absl::Minutes(1), t0);
  obj.Add({1, 2}, t0);
  obj.Add({2, 4}, t0);
  std::vector<double> sum(2);
  obj.SumInto(absl::Span<double>(sum), t0);
  EXPECT_EQ(3, sum[0]);
  EXPECT_EQ(6, sum[1]);
}

TEST(StatsObjectTest, RateInto) {
  const absl::Time t0 = absl::UnixEpoch();
  StatsObject<4> obj(2, absl::Minutes(1), t0);
  obj.Add({1, 2}, t0);
  obj.Add({2, 4}, t0);
  std::vector<double> rate(2);
  obj.RateInto(absl::Span<double>(rate), t0);
  EXPECT_DOUBLE_EQ(3 / 60.0, rate[0]);
  EXPECT_DOUBLE_EQ(6 / 60.0, rate[1]);
}

TEST(StatsObjectTest, SteadyStateDistribution) {
  absl::Time time = absl::UnixEpoch();
  const absl::Duration bucket_duration = absl::Seconds(10);
  uint64_t count;
  double mean;
  double sum_of_squared_deviation;
  double min;
  double max;
  std::vector<uint64_t> histogram(2);
  StatsObject<4> obj(histogram.size() + 5, bucket_duration * 4, time);

  // Add a bit of time to avoid falling exactly on bucket boundaries.
  time += bucket_duration / 2;
  for (int i = 0; i < 10; ++i) {
    obj.AddToDistribution(5, 0, time);
    obj.AddToDistribution(10, 0, time);
    obj.AddToDistribution(15, 1, time);
    obj.DistributionInto(&count, &mean, &sum_of_squared_deviation, &min, &max,
                         absl::Span<uint64_t>(histogram), time);

    // Since we add stats halfway through each bucket, half the oldest bucket
    // counts toward present stats.
    const double buckets_filled = std::min(static_cast<double>(i) + 1, 4.5);
    EXPECT_EQ(count, static_cast<uint64_t>(3 * buckets_filled));
    EXPECT_DOUBLE_EQ(mean, 10.0);
    EXPECT_DOUBLE_EQ(sum_of_squared_deviation, 50 * buckets_filled);
    EXPECT_DOUBLE_EQ(min, 5.0);
    EXPECT_DOUBLE_EQ(max, 15.0);
    EXPECT_THAT(histogram, ::testing::ElementsAre(
                               static_cast<uint64_t>(2 * buckets_filled),
                               static_cast<uint64_t>(buckets_filled)));

    time += bucket_duration;
  }
}

TEST(StatsObjectTest, UnevenDistribution) {
  absl::Time time = absl::UnixEpoch();
  const absl::Duration bucket_duration = absl::Seconds(10);
  uint64_t count;
  double mean;
  double sum_of_squared_deviation;
  double min;
  double max;
  std::vector<uint64_t> histogram(2);
  StatsObject<4> obj(histogram.size() + 5, bucket_duration * 4, time);

  time += bucket_duration / 2;
  obj.AddToDistribution(0, 0, time);
  obj.AddToDistribution(4, 0, time);
  obj.DistributionInto(&count, &mean, &sum_of_squared_deviation, &min, &max,
                       absl::Span<uint64_t>(histogram), time);
  EXPECT_EQ(count, 2);
  EXPECT_DOUBLE_EQ(mean, 2.0);
  EXPECT_DOUBLE_EQ(sum_of_squared_deviation, 8.0);
  EXPECT_DOUBLE_EQ(min, 0.0);
  EXPECT_DOUBLE_EQ(max, 4.0);
  EXPECT_THAT(histogram, ::testing::ElementsAre(2, 0));

  time += bucket_duration;
  obj.AddToDistribution(8, 1, time);
  obj.AddToDistribution(12, 1, time);
  obj.AddToDistribution(16, 1, time);
  obj.DistributionInto(&count, &mean, &sum_of_squared_deviation, &min, &max,
                       absl::Span<uint64_t>(histogram), time);
  EXPECT_EQ(count, 5);
  EXPECT_DOUBLE_EQ(mean, 8.0);
  EXPECT_DOUBLE_EQ(sum_of_squared_deviation, 160.0);
  EXPECT_DOUBLE_EQ(min, 0.0);
  EXPECT_DOUBLE_EQ(max, 16.0);
  EXPECT_THAT(histogram, ::testing::ElementsAre(2, 3));
}

TEST(StatsObjectDeathTest, NonpositiveInterval) {
  EXPECT_DEBUG_DEATH(StatsObject<4>(1, absl::Seconds(0), absl::UnixEpoch()),
                     "");
#ifdef NDEBUG
  EXPECT_EQ(
      StatsObject<4>(1, absl::Seconds(0), absl::UnixEpoch()).total_interval(),
      absl::Seconds(1));
#endif
}

}  // anonymous namespace
}  // namespace common
}  // namespace opencensus
