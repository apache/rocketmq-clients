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

#ifndef OPENCENSUS_COMMON_INTERNAL_STATS_OBJECT_H_
#define OPENCENSUS_COMMON_INTERNAL_STATS_OBJECT_H_

#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#include "absl/base/macros.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/substitute.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "absl/types/span.h"

namespace opencensus {
namespace common {

// StatsObject keeps a rolling sum of a vector of doubles over a period of time.
// This lets you get approximate answers to questions like "how many bytes were
// sent over the past hour?"  One StatsObject can keep multiple distinct
// aggregate stats (e.g. "number of requests" and "number of response bytes"
// could be stored in a single object).
//
// StatsObject<N> divides the period of time of interest into N buckets, each
// covering a period of I seconds, for an I set when the object is constructed.
// It accumulates data into the current bucket, and then expires the oldest
// bucket after I seconds, promoting a new, empty bucket to the current bucket.
// Thus it keeps data for the past N * I seconds.
//
// To make Merge() simpler, StatsObject aligns its bucket boundaries to
// multiples of I, starting from the Unix epoch.
//
// There's an important boundary condition here.  Suppose N = 4 and I = 15, so
// we keep data for the past 4 * 15 = 60s.  If we were to expire our first
// bucket 61s after it was created, we would now have only 46s  of data to
// report.  This would cause our sum to sawtooth with period I.  To get around
// this problem, we keep N + 1 buckets worth of data, and when computing our
// sum, we include a portion of the last bucket equal to the amount of the first
// bucket that hasn't yet been filled.
//
// StatsObject has a notion of the "current time", which is the greatest value
// for 'now' passed to a non-const member function.  If you pass a value for
// 'now' to a const or non-const member function that's less than its "current
// time", the object may implicitly increase 'now', possibly up to the current
// time.
//
// Thread-compatible.
template <uint16_t N>
class StatsObject {
 public:
  // No copy or assign--these cannot be defined reliably.
  StatsObject(const StatsObject<N>&) = delete;
  StatsObject& operator=(const StatsObject<N>&) = delete;

  // The duration covered by one of this object's buckets.
  absl::Duration bucket_interval() const { return bucket_interval_; }
  // The duration covered by this object.
  absl::Duration total_interval() const { return N * bucket_interval_; }

  // Create a new StatsObject keeping num_stats distinct stats over the past
  // 'interval'. 'interval' will be rounded to 1 second if it is smaller.
  StatsObject(uint16_t num_stats, absl::Duration interval, absl::Time now);

  // The number of distinct stats we keep data for.
  uint16_t num_stats() const { return num_stats_; }

  // Gets the sum of each of our stats, as of 'now'.  The returned vector has
  // length num_stats().
  std::vector<double> Sum(absl::Time now) const;

  // Writes the sum of each of our stats, as of 'now', into the given
  // Span, which must have num_stats() elements.
  void SumInto(absl::Span<double> val, absl::Time now) const;

  // Gets the sum of each stat divided by total_interval().
  std::vector<double> Rate(absl::Time now) const;
  void RateInto(absl::Span<double> val, absl::Time now) const;

  // Calculates distribution statistics as of 'now'. This
  // assumes the following specific structure of the StatsObject's stats;
  // StatsObjects using this structure should use AddToDistribution(), not
  // Add(). Count and histogram buckets are rounded to the nearest integer.
  // 0: count
  // 1: mean
  // 2: sum of squared deviation
  // 3: min
  // 4: max
  // 5 through num_histogram_buckets + 4: histogram buckets.
  void DistributionInto(uint64_t* count, double* mean,
                        double* sum_of_squared_deviation, double* min,
                        double* max, absl::Span<uint64_t> histogram_buckets,
                        absl::Time now) const;

  // Are each of our stats zero as of now?  Equivalent to checking whether each
  // element of the vector returned by Sum(now) is zero.
  bool IsEmpty(absl::Time now) const;

  // Fast-forwards this object's current time to 'now', then adds the given data
  // to the current bucket.
  //
  // values.length() must equal num_stats(), otherwise the call is ignored.  If
  // 'now' is less than this object's current time, we simply add 'values' into
  // the current bucket.
  void Add(absl::Span<const double> values, absl::Time now);

  // Fast-forwards this object's current time to 'now' and updates stats based
  // on the provided value and histogram bucket index. Assumes the structure
  // specified in DistributionInto().
  void AddToDistribution(double value, int histogram_bucket, absl::Time now);

  // Fast-forwards this object's current time to 'now' and returns a mutable
  // pointer to the current bucket's data.  This lets you accomplish the same
  // thing as Add() but without constructing an ArraySlice out of your data.
  // The returned Span is valid only until you call a non-const
  // function on this object.
  //
  // The returned array slice has num_stats() elements.
  absl::Span<double> MutableCurrentBucket(absl::Time now);

  // Fast-forwards this object's current time to other's current time if 'other'
  // is ahead of 'this,' then adds all the data from 'other' into this.  If
  // other.num_stats() != this->num_stats() or other.bucket_interval() !=
  // this.bucket_interval(), the call is ignored.
  void Merge(const StatsObject<N>& other);

  std::string DebugString() const;

 private:
  static_assert(N > 0, "Number of buckets must be greater than 0.");

  constexpr uint16_t NumBuckets() const { return N + 1; }
  absl::Time CurBucketStartTime() const {
    return next_bucket_start_time_ - bucket_interval_;
  }

  // Gets the index into the array of the Nth bucket, where the 0th bucket is
  // the current bucket.
  uint32_t NthBucketIndex(uint32_t n) const;

  // Gets the data in the Nth bucket.
  absl::Span<double> NthBucket(uint32_t n);
  absl::Span<const double> NthBucket(uint32_t n) const;

  // By how many bucket intervals is 'now' ahead of the current bucket?  Returns
  // 0 if 'now' is behind the current bucket, or numeric_limits<uint32_t>::max()
  // if now is way ahead of the current bucket.
  //
  // If the returned value isn't saturated to numeric_limits<uint32_t>::max(),
  // the following inequality is satisfied
  //   next_bucket_start_time_ + bucket_interval_ * BucketsAhead(now) > now.
  uint32_t BucketsAhead(absl::Time now) const {
    double v = std::max(
        0.0, std::floor(absl::FDivDuration(now - next_bucket_start_time_,
                                           bucket_interval_) +
                        1));
    // This will fail if now is so large that
    //   (now / bucket_interval_) + 1 == (now / bucket_interval_),
    // but this whole class is liable to fall apart in that case.
    ABSL_ASSERT(next_bucket_start_time_ + bucket_interval_ * v > now);
    return v <= std::numeric_limits<uint32_t>::max()
               ? static_cast<uint32_t>(v)
               : std::numeric_limits<uint32_t>::max();
  }

  // Shifts our data forward in time so that next_bucket_start_time > now.
  void Shift(absl::Time now);

  // Returns the proportion (in [0,1]) of the oldest bucket to add based on how
  // much time has passed in the most recent bucket and
  // initial_bucket_fraction_filled_.
  double LastBucketPortion(absl::Time now) const;

  // The interval covered by each bucket.
  const absl::Duration bucket_interval_;
  // We use uint16_t and float for num_stats_, cur_bucket_, and
  // initial_bucket_fraction_filled_ so they'll pack into 8 bytes.
  const uint16_t num_stats_;
  // Index of the current bucket in data_.  That is, the current bucket's data
  // is stored in the num_stats_ elements starting at
  // data_.data() + cur_bucket_ * num_stats_.
  uint16_t cur_bucket_;
  // initial_bucket_fraction_filled_ helps us solve a particular data
  // interpolation problem which occurs when the object has roughly N * I
  // seconds' worth of data.
  //
  // Suppose bucket_interval_ = 15 and we create an object at t = 20 seconds.
  // StatsObject aligns its intervals to multiples of bucket_interval_,
  // so the first interval will go from t = 20s to t = 30s, which means that the
  // very first bucket will record data for 30 - 20 = 10s, instead of the normal
  // 15s. In this case, we set initial_bucket_fraction_filled_ to 10 / 15 =
  // 0.667 in our constructor, and set it to 0 once the very first bucket has
  // been shifted out.
  float initial_bucket_fraction_filled_;
  // Start time of the bucket after the one we're currently recording stats for.
  // Always a multiple of bucket_interval_.  This means that the current bucket
  // records stats for the half-open interval
  //   [next_bucket_start_time_ - bucket_interval_, next_bucket_start_time_)
  // The fact that this interval is open on the RHS is significant to
  // BucketsAhead()!
  absl::Time next_bucket_start_time_;
  // Stores this object's data.  Bucket b contains num_stats_ elements at
  // indices [b * num_stats_, (b + 1) * num_stats_).
  std::vector<double> data_;
};

template <uint16_t N>
StatsObject<N>::StatsObject(uint16_t num_stats, absl::Duration interval,
                            absl::Time now)
    : bucket_interval_(std::max(interval, absl::Seconds(1)) / N),
      num_stats_(num_stats),
      cur_bucket_(0),
      data_(num_stats * (N + 1)) {
  ABSL_ASSERT(interval >= absl::Seconds(1) &&
              "Too small stats object interval");
  absl::Time cur_bucket_start_time =
      absl::UnixEpoch() +
      absl::Floor(now - absl::UnixEpoch(), bucket_interval_);
  next_bucket_start_time_ = cur_bucket_start_time + bucket_interval_;
  initial_bucket_fraction_filled_ =
      1 - absl::FDivDuration(now - cur_bucket_start_time, bucket_interval_);
}

template <uint16_t N>
uint32_t StatsObject<N>::NthBucketIndex(uint32_t n) const {
  int32_t bucket = cur_bucket_ - n;
  if (bucket < 0) {
    bucket += NumBuckets();
  }
  ABSL_ASSERT(bucket == (cur_bucket_ + NumBuckets() - n) % NumBuckets());
  return bucket;
}

template <uint16_t N>
absl::Span<double> StatsObject<N>::NthBucket(uint32_t n) {
  return absl::Span<double>(data_.data() + NthBucketIndex(n) * num_stats_,
                            num_stats_);
}

template <uint16_t N>
absl::Span<const double> StatsObject<N>::NthBucket(uint32_t n) const {
  return absl::Span<const double>(data_.data() + NthBucketIndex(n) * num_stats_,
                                  num_stats_);
}

template <uint16_t N>
void StatsObject<N>::Add(absl::Span<const double> values, absl::Time now) {
  if (values.length() != num_stats_) {
    std::cerr << "values has the wrong number of elements; expected "
              << num_stats_ << ", but was " << values.length() << "\n";
    return;
  }
  absl::Span<double> bucket = MutableCurrentBucket(now);
  for (uint32_t i = 0; i < num_stats_; ++i) {
    bucket[i] += values[i];
  }
}

template <uint16_t N>
void StatsObject<N>::AddToDistribution(double value, int histogram_bucket,
                                       absl::Time now) {
  ABSL_ASSERT(num_stats_ >= histogram_bucket + 5);
  absl::Span<double> bucket = MutableCurrentBucket(now);
  const double old_count = bucket[0];
  const double count = ++bucket[0];
  const double old_mean = bucket[1];
  const double new_mean = old_mean + (value - old_mean) / count;
  bucket[2] += (value - old_mean) * (value - new_mean);
  bucket[1] = new_mean;
  if (old_count) {
    bucket[3] = std::min(value, bucket[3]);
    bucket[4] = std::max(value, bucket[4]);
  } else {
    // Simply overwrite if this is the first value added to the bucket.
    bucket[3] = value;
    bucket[4] = value;
  }
  ++bucket[histogram_bucket + 5];
}

template <uint16_t N>
absl::Span<double> StatsObject<N>::MutableCurrentBucket(absl::Time now) {
  Shift(now);
  if (now < CurBucketStartTime()) {
    std::cerr
        << "now=" << now << " < CurBucketStartTime()=" << CurBucketStartTime()
        << "; returning current bucket anyway.  If the difference is small it "
           "might be due to an inconsequential clock perturbation, but if you "
           "see this warning often, it is likely a bug.\n";
  }
  return absl::Span<double>(data_.data() + cur_bucket_ * num_stats_,
                            num_stats_);
}

template <uint16_t N>
std::vector<double> StatsObject<N>::Sum(absl::Time now) const {
  std::vector<double> sum(num_stats_);
  SumInto(absl::Span<double>(sum), now);
  return sum;
}

template <uint16_t N>
double StatsObject<N>::LastBucketPortion(absl::Time now) const {
  // Compute the portion of the requested bucket's interval that has passed.  We
  // interpolate the remainder of the interval's data from the last bucket.
  const double requested_bucket_portion = absl::FDivDuration(
      (now - absl::UnixEpoch()) % bucket_interval_, bucket_interval_);

  // To understand the computation below, first consider the common case when
  // initial_bucket_fraction_filled_ == 0.  This happens after the object has
  // seen more than NumBuckets() intervals pass.  In this case,
  // last_bucket_fraction_filled = 1, and
  // last_bucket_portion = 1 - cur_bucket_portion.  Simple interpolation.
  //
  // Now, if the object has seen strictly fewer than NumBuckets() intervals
  // pass, the last bucket will be empty, and it doesn't matter what value we
  // choose for last_bucket_portion, because we're going to multiply it by zero.
  //
  // The interesting case is when the object has seen exactly NumBuckets()
  // intervals pass.  In this case, we want to take a fraction of the last
  // bucket that corresponds to time - cur_bucket_start_time seconds, unless
  // that value exceeds the number of seconds in the last bucket
  // (bucket_interval_ - first_bucket_lag_seconds), in which case we should
  // just take all of the last bucket.
  return std::min(
      1.0, (1 - requested_bucket_portion) / initial_bucket_fraction_filled_);
}

template <uint16_t N>
void StatsObject<N>::SumInto(absl::Span<double> val, absl::Time now) const {
  ABSL_ASSERT(val.size() >= num_stats_);
  if (val.size() < num_stats_) {
    std::fill(val.begin(), val.end(), 0);
    return;
  }
  std::fill(val.begin(), val.begin() + num_stats_, 0);
  const uint32_t buckets_ahead = BucketsAhead(now);
  if (buckets_ahead >= NumBuckets()) {
    return;
  }

  for (uint32_t i = 0; i < NumBuckets() - 1 - buckets_ahead; ++i) {
    absl::Span<const double> bucket = NthBucket(i);
    for (uint32_t j = 0; j < num_stats_; ++j) {
      val[j] += bucket[j];
    }
  }

  // Now add (possibly only a part of) the data from the last bucket.
  const double last_bucket_portion = LastBucketPortion(now);
  absl::Span<const double> last_bucket =
      NthBucket(NumBuckets() - 1 - buckets_ahead);
  for (uint32_t i = 0; i < num_stats_; ++i) {
    val[i] += last_bucket_portion * last_bucket[i];
  }
}

template <uint16_t N>
std::vector<double> StatsObject<N>::Rate(absl::Time now) const {
  std::vector<double> ret(num_stats_);
  RateInto(absl::Span<double>(ret), now);
  return ret;
}

template <uint16_t N>
void StatsObject<N>::RateInto(absl::Span<double> val, absl::Time now) const {
  SumInto(absl::Span<double>(val), now);
  size_t max_bucket = std::min<size_t>(val.size(), NumBuckets());
  for (uint32_t i = 0; i < max_bucket; ++i) {
    val[i] /= absl::ToDoubleSeconds(total_interval());
  }
}

template <uint16_t N>
void StatsObject<N>::DistributionInto(uint64_t* count, double* mean,
                                      double* sum_of_squared_deviation,
                                      double* min, double* max,
                                      absl::Span<uint64_t> histogram_buckets,
                                      absl::Time now) const {
  ABSL_ASSERT(histogram_buckets.size() + 5 == num_stats_);
  const uint32_t buckets_ahead = BucketsAhead(now);
  *count = 0;
  *mean = 0;
  *sum_of_squared_deviation = 0;
  *min = std::numeric_limits<double>::infinity();
  *max = -std::numeric_limits<double>::infinity();
  std::fill(histogram_buckets.begin(), histogram_buckets.end(), 0);
  if (histogram_buckets.size() < num_stats_ - 5 ||
      buckets_ahead >= NumBuckets()) {
    return;
  }

  // Updates stats with a new bucket, scaling it by scaling_factor.
  const auto UpdateFromBucket =
      [count, mean, sum_of_squared_deviation, min, max, histogram_buckets](
          absl::Span<const double> bucket, double scaling_factor) {
        // Skip empty buckets, since they have not been initialized correctly.
        if (!bucket[0]) {
          return;
        }
        // Combine statistics using the parallel algorithm.
        const double delta = bucket[1] - *mean;
        const double bucket_count = bucket[0] * scaling_factor;
        const double bucket_sum_of_squared_deviation =
            bucket[2] * scaling_factor;
        *sum_of_squared_deviation =
            *sum_of_squared_deviation + bucket_sum_of_squared_deviation +
            pow(delta, 2) * *count * bucket_count / (*count + bucket_count);
        *mean = ((*mean * *count) + (bucket[1] * bucket_count)) /
                (*count + bucket_count);
        // Note that scaling_factor will be one for all but the last update and
        // the count and histogram buckets should be integers, so this is
        // equivalent to summing to a double and then rounding.
        *count += bucket_count;
        *min = std::min(*min, bucket[3]);
        *max = std::max(*max, bucket[4]);
        for (int i = 0; i < histogram_buckets.size(); ++i) {
          histogram_buckets[i] += bucket[i + 5] * scaling_factor;
        }
      };

  for (uint32_t i = 0; i < NumBuckets() - 1 - buckets_ahead; ++i) {
    UpdateFromBucket(NthBucket(i), 1.0);
  }

  // Now add (possibly only a part of) the data from the last bucket.
  const double last_bucket_portion = LastBucketPortion(now);
  absl::Span<const double> last_bucket =
      NthBucket(NumBuckets() - 1 - buckets_ahead);
  UpdateFromBucket(last_bucket, last_bucket_portion);
}

template <uint16_t N>
bool StatsObject<N>::IsEmpty(absl::Time now) const {
  const int32_t buckets_ahead = BucketsAhead(now);
  for (int32_t i = 0; i < NumBuckets() - buckets_ahead; ++i) {
    for (const double& val : NthBucket(i)) {
      if (val != 0) {
        return false;
      }
    }
  }
  return true;
}

template <uint16_t N>
void StatsObject<N>::Shift(absl::Time now) {
  if (now < next_bucket_start_time_) {
    return;
  }

  uint64_t num_shifts = BucketsAhead(now);
  uint32_t num_buckets_to_clear = std::min<uint32_t>(NumBuckets(), num_shifts);
  for (uint32_t i = 0; i < num_buckets_to_clear; ++i) {
    absl::Span<double> bucket = NthBucket(NumBuckets() - i - 1);
    std::fill(bucket.begin(), bucket.end(), 0);
  }

  // cur_bucket_ starts at 0, so this checks whether we've seen more than
  // NumBuckets() total shifts.  If so, the initial bucket is gone.
  if (num_buckets_to_clear + cur_bucket_ >= NumBuckets()) {
    initial_bucket_fraction_filled_ = 1;
  }
  cur_bucket_ = NthBucketIndex(NumBuckets() - num_buckets_to_clear);
  // We could equivalently write
  // next_bucket_start_time_ += bucket_interval_ * num_shifts;,
  // but that doesn't work when now is much larger than next_bucket_time_,
  // saturating num_shifts to numeric_limits<uint32_t>::max.
  next_bucket_start_time_ =
      absl::UnixEpoch() +
      absl::Floor(now - absl::UnixEpoch(), bucket_interval_) + bucket_interval_;
  ABSL_ASSERT(now < next_bucket_start_time_);
}

template <uint16_t N>
void StatsObject<N>::Merge(const StatsObject<N>& other) {
  if (num_stats_ != other.num_stats_) {
    std::cerr << "num_stats_ mismatch: Expected " << num_stats_ << ", but was "
              << other.num_stats_ << "\n";
    return;
  }
  if (bucket_interval_ != other.bucket_interval_) {
    std::cerr << "bucket_interval_ mismatch: Expected " << bucket_interval_
              << ", but was " << other.bucket_interval_ << "\n";
    return;
  }
  Shift(other.CurBucketStartTime());
  ABSL_ASSERT(next_bucket_start_time_ >= other.next_bucket_start_time_);
  // We guaranteed that our data isn't behind other's by calling Shift() above,
  // but our data may still be ahead of other's.
  uint32_t intervals_ahead = other.BucketsAhead(CurBucketStartTime());
  if (intervals_ahead >= NumBuckets()) {
    return;
  }

  initial_bucket_fraction_filled_ = std::max(
      other.initial_bucket_fraction_filled_, initial_bucket_fraction_filled_);

  for (uint32_t i = 0; i < NumBuckets() - intervals_ahead; ++i) {
    absl::Span<double> this_bucket = NthBucket(i + intervals_ahead);
    absl::Span<const double> other_bucket = other.NthBucket(i);
    for (uint32_t j = 0; j < num_stats_; ++j) {
      this_bucket[j] += other_bucket[j];
    }
  }
}

template <uint16_t N>
std::string StatsObject<N>::DebugString() const {
  std::string s =
      absl::Substitute("StatsObject<$0> with $2 stat$3 over $1 intervals.", N,
                       absl::FormatDuration(bucket_interval_), num_stats(),
                       num_stats() == 1 ? "" : "s");
  for (uint32_t stat = 0; stat < num_stats(); ++stat) {
    absl::StrAppend(&s, "\n");
    if (num_stats() > 1) {
      absl::StrAppend(&s, "Stat ", stat, ": ");
    }
    absl::StrAppend(&s, "{");
    for (uint32_t bucket = 0; bucket < NumBuckets(); ++bucket) {
      absl::StrAppend(&s, bucket > 0 ? ", " : "", NthBucket(bucket)[stat]);
    }
    absl::StrAppend(&s, "}");
  }
  absl::StrAppend(&s, "\nnext_bucket_start_time_ = ",
                  absl::FormatTime(next_bucket_start_time_), ", ",
                  absl::FormatDuration(next_bucket_start_time_ - absl::Now()),
                  " from now.");
  if (initial_bucket_fraction_filled_ != 1) {
    absl::StrAppend(&s, "\ninitial_bucket_fraction_filled_ = ",
                    initial_bucket_fraction_filled_);
  }
  return s;
}

}  // namespace common
}  // namespace opencensus

#endif  // OPENCENSUS_COMMON_INTERNAL_STATS_OBJECT_H_
