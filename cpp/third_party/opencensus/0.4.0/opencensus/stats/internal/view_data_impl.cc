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
          }
        case Aggregation::Type::kCount:
          return ViewDataImpl::Type::kInt64;
        case Aggregation::Type::kDistribution:
          return ViewDataImpl::Type::kDistribution;
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
      start_time_(std::max(other.start_time(),
                           now - other.aggregation_window().duration())),
      end_time_(now) {
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
      start_time_(other.start_time_),
      end_time_(other.end_time_) {
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
  end_time_ = std::max(end_time_, now);
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
      start_time_(source->start_time_),
      end_time_(now) {
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
  source->start_time_ = now;
  source->end_time_ = now;
}

}  // namespace stats
}  // namespace opencensus
