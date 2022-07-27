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

#ifndef OPENCENSUS_STATS_INTERNAL_VIEW_DATA_IMPL_H_
#define OPENCENSUS_STATS_INTERNAL_VIEW_DATA_IMPL_H_

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/base/macros.h"
#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "opencensus/common/internal/stats_object.h"
#include "opencensus/common/internal/string_vector_hash.h"
#include "opencensus/stats/aggregation.h"
#include "opencensus/stats/distribution.h"
#include "opencensus/stats/internal/aggregation_window.h"
#include "opencensus/stats/internal/measure_data.h"
#include "opencensus/stats/view_descriptor.h"

namespace opencensus {
namespace stats {

// ViewDataImpl contains a snapshot of data for a particular View. DataValueT is
// the type of the returned data, with possibilities listed in
// data_value_type.h. Which value type is returned for a view is determined by
// the view's aggregation and aggregation window.
//
// Thread-compatible.
class ViewDataImpl {
 public:
  // A convenience alias for the type of the map from tags to data.
  template <typename DataValueT>
  using DataMap = std::unordered_map<std::vector<std::string>, DataValueT,
                                     common::StringVectorHash>;
  // 4 is the number of buckets to group observations into (see
  // opencensus/common/internal/stats_object.h for details)--this balances the
  // precision of estimates against resource use.
  typedef common::StatsObject<4> IntervalStatsObject;

  // Constructs an empty ViewDataImpl for internal use from the descriptor. A
  // ViewData can be constructed directly from such a ViewDataImpl for
  // snapshotting cumulative data; ViewDataImpls for interval views must be
  // converted using the following constructor before snapshotting.
  ViewDataImpl(absl::Time start_time, const ViewDescriptor& descriptor);
  // Constructs a ViewDataImpl capturing the state of 'other' at 'now'. Requires
  // 'other' to have an interval aggregation window (and thus type()
  // kStatsObject).
  ViewDataImpl(const ViewDataImpl& other, absl::Time now);

  ViewDataImpl(const ViewDataImpl& other);
  ~ViewDataImpl();

  // Returns a copy of the present state of the object and resets data() and
  // start_time().
  std::unique_ptr<ViewDataImpl> GetDeltaAndReset(absl::Time now);

  const Aggregation& aggregation() const { return aggregation_; }
  const AggregationWindow& aggregation_window() const {
    return aggregation_window_;
  }

  enum class Type {
    kDouble,
    kInt64,
    kDistribution,
    kStatsObject,  // Used for aggregating data, should not be exported.
  };
  Type type() const { return type_; }

  // A map from tag values (corresponding to the keys in the ViewDescriptor, in
  // that order) to the data for those tags. What data is contained depends on
  // the View's Aggregation and AggregationWindow.
  // Only one of these is valid for any ViewDataImpl (which is indicated by
  // type());
  const DataMap<double>& double_data() const {
    ABSL_ASSERT(type_ == Type::kDouble);
    return double_data_;
  }
  const DataMap<int64_t>& int_data() const {
    ABSL_ASSERT(type_ == Type::kInt64);
    return int_data_;
  }
  const DataMap<Distribution>& distribution_data() const {
    ABSL_ASSERT(type_ == Type::kDistribution);
    return distribution_data_;
  }
  const DataMap<IntervalStatsObject>& interval_data() const {
    ABSL_ASSERT(type_ == Type::kStatsObject);
    return interval_data_;
  }

  // Returns a start time for each timeseries/tag map.
  const DataMap<absl::Time>& start_times() const { return start_times_; }

  // DEPRECATED: Legacy start_time_ for the entire view.
  // This should be deleted if custom exporters are updated to
  // use start_times_ and stop depending on this field.
  absl::Time start_time() const { return start_time_; }

  // Merges bulk data for the given tag values at 'now'. tag_values must be
  // ordered according to the order of keys in the ViewDescriptor.
  // TODO: Change to take Span<string_view> when heterogenous lookup is
  // supported.
  void Merge(const std::vector<std::string>& tag_values,
             const MeasureData& data, absl::Time now);

 private:
  // Implements GetDeltaAndReset(), copying aggregation_ and swapping data_ and
  // start/end times. This is private so that it can be given a more descriptive
  // name in the public API.
  ViewDataImpl(ViewDataImpl* source, absl::Time now);

  Type TypeForDescriptor(const ViewDescriptor& descriptor);

  void SetStartTimeIfUnset(const std::vector<std::string>& tag_values,
                           absl::Time now) {
    // If the time is not set.
    if (start_times_.find(tag_values) == start_times_.end()) {
      start_times_[tag_values] = now;
    }
  }

  // If expiry duration is set, keep track of the last update time of each view
  // data.
  void SetUpdateTime(const std::vector<std::string>& tag_values,
                     absl::Time now);

  // Purge view data that has not been updated for expiry duration.
  void PurgeExpired(absl::Time now);

  const Aggregation aggregation_;
  const AggregationWindow aggregation_window_;
  const Type type_;
  union {
    DataMap<double> double_data_;
    DataMap<int64_t> int_data_;
    DataMap<Distribution> distribution_data_;
    DataMap<IntervalStatsObject> interval_data_;
  };

  // A start time for each timeseries/tag map.
  DataMap<absl::Time> start_times_;

  using UpdateTimeList =
      std::list<std::pair<absl::Time, std::vector<std::string>>>;

  // A list of last update time for each timeseries.
  UpdateTimeList update_times_;
  // A map from view data tags to the last update time list iterator.
  DataMap<UpdateTimeList::iterator> update_time_entries_;

  const absl::Duration expiry_duration_;

  // DEPRECATED: Legacy start_time_ for the entire view.
  // This should be deleted if custom exporters are updated to
  // use start_times_ and stop depending on this field
  absl::Time start_time_;
};

}  // namespace stats
}  // namespace opencensus

#endif  // OPENCENSUS_STATS_INTERNAL_VIEW_DATA_IMPL_H_
