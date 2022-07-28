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

#include "opencensus/stats/internal/view_data_impl.h"

#include <cstdint>
#include <iostream>
#include <memory>

#include "absl/base/macros.h"
#include "absl/memory/memory.h"
#include "opencensus/stats/distribution.h"
#include "opencensus/stats/measure_descriptor.h"
#include "opencensus/stats/view_descriptor.h"

namespace opencensus {
namespace stats {

ViewDataImpl::Type ViewDataImpl::TypeForDescriptor(
    const ViewDescriptor& descriptor) {
  switch (descriptor.aggregation_window_.type()) {
    case AggregationWindow::Type::kCumulative:
    case AggregationWindow::Type::kDelta:
      switch (descriptor.aggregation().type()) {
        case Aggregation::Type::kSum:
        case Aggregation::Type::kLastValue:
          switch (descriptor.measure_descriptor().type()) {
            case MeasureDescriptor::Type::kDouble:
              return ViewDataImpl::Type::kDouble;
            case MeasureDescriptor::Type::kInt64:
              return ViewDataImpl::Type::kInt64;
            default:
              ABSL_ASSERT(false && "Unknown measure_descriptor type.");
              return ViewDataImpl::Type::kDouble;
          }
        case Aggregation::Type::kCount:
          return ViewDataImpl::Type::kInt64;
        case Aggregation::Type::kDistribution:
          return ViewDataImpl::Type::kDistribution;
        default:
          ABSL_ASSERT(false && "Unknown aggregation type.");
          return ViewDataImpl::Type::kDouble;
      }
    case AggregationWindow::Type::kInterval:
      return ViewDataImpl::Type::kStatsObject;
  }
  ABSL_ASSERT(false && "Bad ViewDataImpl type.");
  return ViewDataImpl::Type::kDouble;
}

ViewDataImpl::ViewDataImpl(absl::Time start_time,
                           const ViewDescriptor& descriptor)
    : aggregation_(descriptor.aggregation()),
      aggregation_window_(descriptor.aggregation_window_),
      type_(TypeForDescriptor(descriptor)),
      start_times_(),
      expiry_duration_(descriptor.expiry_duration_),
      start_time_(start_time) {
  switch (type_) {
    case Type::kDouble: {
      new (&double_data_) DataMap<double>();
      break;
    }
    case Type::kInt64: {
      new (&int_data_) DataMap<int64_t>();
      break;
    }
    case Type::kDistribution: {
      new (&distribution_data_) DataMap<Distribution>();
      break;
    }
    case Type::kStatsObject: {
      new (&interval_data_) DataMap<IntervalStatsObject>();
      break;
    }
  }
}

ViewDataImpl::ViewDataImpl(const ViewDataImpl& other, absl::Time now)
    : aggregation_(other.aggregation()),
      aggregation_window_(other.aggregation_window()),
      type_(other.aggregation().type() == Aggregation::Type::kDistribution
                ? Type::kDistribution
                : Type::kDouble),
      start_times_(),
      start_time_(std::max(other.start_time(),
                           now - other.aggregation_window().duration())) {
  // Intentionally reset the source with a new start time.
  for (const auto& it : other.start_times_) {
    auto new_start_time =
        std::max(it.second, now - other.aggregation_window().duration());
    start_times_[it.first] = new_start_time;
  }

  ABSL_ASSERT(aggregation_window_.type() == AggregationWindow::Type::kInterval);

  switch (aggregation_.type()) {
    case Aggregation::Type::kSum:
    case Aggregation::Type::kCount: {
      new (&double_data_) DataMap<double>();
      for (const auto& row : other.interval_data()) {
        row.second.SumInto(absl::Span<double>(&double_data_[row.first], 1),
                           now);
      }
      break;
    }
    case Aggregation::Type::kDistribution: {
      new (&distribution_data_) DataMap<Distribution>();
      for (const auto& row : other.interval_data()) {
        const std::pair<DataMap<Distribution>::iterator, bool>& it =
            distribution_data_.emplace(
                row.first, Distribution(&aggregation_.bucket_boundaries()));
        Distribution& distribution = it.first->second;
        row.second.DistributionInto(
            &distribution.count_, &distribution.mean_,
            &distribution.sum_of_squared_deviation_, &distribution.min_,
            &distribution.max_,
            absl::Span<uint64_t>(distribution.bucket_counts_), now);
      }
      break;
    }
    case Aggregation::Type::kLastValue:
      std::cerr << "Interval/LastValue is not supported.\n";
      ABSL_ASSERT(0 && "Interval/LastValue is not supported.\n");
      break;
  }
}

ViewDataImpl::~ViewDataImpl() {
  switch (type_) {
    case Type::kDouble: {
      double_data_.~DataMap<double>();
      break;
    }
    case Type::kInt64: {
      int_data_.~DataMap<int64_t>();
      break;
    }
    case Type::kDistribution: {
      distribution_data_.~DataMap<Distribution>();
      break;
    }
    case Type::kStatsObject: {
      interval_data_.~DataMap<IntervalStatsObject>();
      break;
    }
  }
}

std::unique_ptr<ViewDataImpl> ViewDataImpl::GetDeltaAndReset(absl::Time now) {
  // Need to use wrap_unique because this is a private constructor.
  return absl::WrapUnique(new ViewDataImpl(this, now));
}

ViewDataImpl::ViewDataImpl(const ViewDataImpl& other)
    : aggregation_(other.aggregation_),
      aggregation_window_(other.aggregation_window_),
      type_(other.type()),
      start_times_(other.start_times_),
      start_time_(other.start_time_) {
  switch (type_) {
    case Type::kDouble: {
      new (&double_data_) DataMap<double>(other.double_data_);
      break;
    }
    case Type::kInt64: {
      new (&int_data_) DataMap<int64_t>(other.int_data_);
      break;
    }
    case Type::kDistribution: {
      new (&distribution_data_) DataMap<Distribution>(other.distribution_data_);
      break;
    }
    case Type::kStatsObject: {
      std::cerr
          << "StatsObject ViewDataImpl cannot (and should not) be copied. "
             "(Possibly failed to convert to export data type?)";
      ABSL_ASSERT(0);
      break;
    }
  }
}

void ViewDataImpl::Merge(const std::vector<std::string>& tag_values,
                         const MeasureData& data, absl::Time now) {
  // A value is set here. Set a start time if it is unset.
  SetStartTimeIfUnset(tag_values, now);
  SetUpdateTime(tag_values, now);
  PurgeExpired(now);
  switch (type_) {
    case Type::kDouble: {
      if (aggregation_.type() == Aggregation::Type::kSum) {
        double_data_[tag_values] += data.sum();
      } else {
        ABSL_ASSERT(aggregation_.type() == Aggregation::Type::kLastValue);
        double_data_[tag_values] = data.last_value();
      }
      break;
    }
    case Type::kInt64: {
      switch (aggregation_.type()) {
        case Aggregation::Type::kCount: {
          int_data_[tag_values] += data.count();
          break;
        }
        case Aggregation::Type::kSum: {
          int_data_[tag_values] += data.sum();
          break;
        }
        case Aggregation::Type::kLastValue: {
          int_data_[tag_values] = data.last_value();
          break;
        }
        default:
          ABSL_ASSERT(false && "Invalid aggregation for type.");
      }
      break;
    }
    case Type::kDistribution: {
      DataMap<Distribution>::iterator it = distribution_data_.find(tag_values);
      if (it == distribution_data_.end()) {
        it = distribution_data_.emplace_hint(
            it, tag_values, Distribution(&aggregation_.bucket_boundaries()));
      }
      data.AddToDistribution(&it->second);
      break;
    }
    case Type::kStatsObject: {
      DataMap<IntervalStatsObject>::iterator it =
          interval_data_.find(tag_values);
      if (aggregation_.type() == Aggregation::Type::kDistribution) {
        const auto& buckets = aggregation_.bucket_boundaries();
        if (it == interval_data_.end()) {
          it = interval_data_.emplace_hint(
              it, std::piecewise_construct, std::make_tuple(tag_values),
              std::make_tuple(buckets.num_buckets() + 5,
                              aggregation_window_.duration(), now));
        }
        auto window = it->second.MutableCurrentBucket(now);
        data.AddToDistribution(
            buckets, &window[0], &window[1], &window[2], &window[3], &window[4],
            absl::Span<double>(&window[5], buckets.num_buckets()));
      } else {
        if (it == interval_data_.end()) {
          it = interval_data_.emplace_hint(
              it, std::piecewise_construct, std::make_tuple(tag_values),
              std::make_tuple(1, aggregation_window_.duration(), now));
        }
        if (aggregation_ == Aggregation::Count()) {
          it->second.MutableCurrentBucket(now)[0] += data.count();
        } else {
          it->second.MutableCurrentBucket(now)[0] += data.sum();
        }
      }
      break;
    }
  }
}

ViewDataImpl::ViewDataImpl(ViewDataImpl* source, absl::Time now)
    : aggregation_(source->aggregation_),
      aggregation_window_(source->aggregation_window_),
      type_(source->type_),
      start_times_(source->start_times_),
      start_time_(source->start_time_) {
  switch (type_) {
    case Type::kDouble: {
      new (&double_data_) DataMap<double>();
      double_data_.swap(source->double_data_);
      break;
    }
    case Type::kInt64: {
      new (&int_data_) DataMap<int64_t>();
      int_data_.swap(source->int_data_);
      break;
    }
    case Type::kDistribution: {
      new (&distribution_data_) DataMap<Distribution>();
      distribution_data_.swap(source->distribution_data_);
      break;
    }
    case Type::kStatsObject: {
      std::cerr << "GetDeltaAndReset should not be called on ViewDataImpl for "
                   "interval stats.";
      ABSL_ASSERT(0);
      break;
    }
  }
  // Intentionally reset the source with new start times.
  source->start_time_ = now;

  for (const auto& it : source->start_times_) {
    source->start_times_[it.first] = now;
  }
}

void ViewDataImpl::SetUpdateTime(const std::vector<std::string>& tag_values,
                                 absl::Time now) {
  if (expiry_duration_ == absl::ZeroDuration()) {
    // No need to track last update time if expiry duration is not set.
    return;
  }

  auto update_time_map_iter = update_time_entries_.find(tag_values);
  if (update_time_map_iter == update_time_entries_.end()) {
    // The timeseries is not tracked, add it to the update time list and map.
    update_times_.emplace_front(now, tag_values);
    update_time_entries_[tag_values] = update_times_.begin();
  } else {
    // The timeseries is tracked, update its updated time and move it to the
    // front of the list.
    auto update_time_list_iter = update_time_map_iter->second;
    update_time_list_iter->first = now;
    update_times_.splice(update_times_.begin(), update_times_,
                         update_time_list_iter);
  }
}

void ViewDataImpl::PurgeExpired(absl::Time now) {
  if (expiry_duration_ == absl::ZeroDuration() || update_times_.empty()) {
    // No need to remove expired entries since either expiry is not set or there
    // is no data entry yet.
    return;
  }
  // Remove data that has not been updated for expiry.
  while (now - update_times_.back().first > expiry_duration_) {
    const auto& tags = update_times_.back().second;
    update_time_entries_.erase(tags);
    start_times_.erase(tags);
    switch (type_) {
      case Type::kDouble: {
        double_data_.erase(tags);
        break;
      }
      case Type::kInt64: {
        int_data_.erase(tags);
        break;
      }
      case Type::kDistribution: {
        distribution_data_.erase(tags);
        break;
      }
      case Type::kStatsObject: {
        interval_data_.erase(tags);
        break;
      }
    }
    update_times_.pop_back();
  }
}

}  // namespace stats
}  // namespace opencensus
