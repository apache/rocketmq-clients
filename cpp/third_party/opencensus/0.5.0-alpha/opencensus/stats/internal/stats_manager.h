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

#ifndef OPENCENSUS_STATS_INTERNAL_STATS_MANAGER_H_
#define OPENCENSUS_STATS_INTERNAL_STATS_MANAGER_H_

#include <memory>

#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "absl/types/span.h"
#include "opencensus/common/internal/stats_object.h"
#include "opencensus/stats/distribution.h"
#include "opencensus/stats/internal/delta_producer.h"
#include "opencensus/stats/internal/measure_data.h"
#include "opencensus/stats/internal/view_data_impl.h"
#include "opencensus/stats/measure.h"
#include "opencensus/stats/view_descriptor.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/tags/tag_map.h"

namespace opencensus {
namespace stats {

// StatsManager is a singleton class that stores data for active views, adding
// values from Record() events.
class StatsManager final {
 public:
  // ViewInformation stores part of the data of a ViewDescriptor
  // (measure, aggregation, and columns), along with the data for the view.
  // ViewInformation is thread-compatible; its non-const data is protected by an
  // external mutex, which most non-const member functions require holding.
  class ViewInformation {
   public:
    ViewInformation(const ViewDescriptor& descriptor, absl::Mutex* mu);

    // Returns true if this ViewInformation can be used to provide data for
    // 'descriptor' (i.e. shares measure, aggregation, aggregation window, and
    // columns; this does not compare view name and description).
    bool Matches(const ViewDescriptor& descriptor) const;

    int num_consumers() const;
    // Increments the consumer count. Requires holding *mu_.
    void AddConsumer();
    // Decrements the consumer count and returns the resulting count. Requires
    // holding *mu_.
    int RemoveConsumer();

    // Adds 'data' under 'tags' as of 'now'. Requires holding *mu_;
    void MergeMeasureData(const opencensus::tags::TagMap& tags,
                          const MeasureData& data, absl::Time now);

    // Retrieves a copy of the data.
    std::unique_ptr<ViewDataImpl> GetData() ABSL_LOCKS_EXCLUDED(*mu_);

    const ViewDescriptor& view_descriptor() const { return descriptor_; }

   private:
    const ViewDescriptor descriptor_;

    absl::Mutex* const mu_;  // Not owned.
    // The number of View objects backed by this ViewInformation, for
    // reference-counted GC.
    int num_consumers_ ABSL_GUARDED_BY(*mu_) = 1;

    // Possible types of stored data.
    enum class DataType { kDouble, kUint64, kDistribution, kInterval };
    static DataType DataTypeForDescriptor(const ViewDescriptor& descriptor);

    ViewDataImpl data_ ABSL_GUARDED_BY(*mu_);
  };

 public:
  static StatsManager* Get();

  // Merges all data from 'delta' at the present time.
  void MergeDelta(const Delta& delta) ABSL_LOCKS_EXCLUDED(mu_);

  // Adds a measure--this is necessary for views to be added under that measure.
  template <typename MeasureT>
  void AddMeasure(Measure<MeasureT> measure) ABSL_LOCKS_EXCLUDED(mu_);

  // Returns a handle that can be used to retrieve data for 'descriptor' (which
  // may point to a new or re-used ViewInformation).
  ViewInformation* AddConsumer(const ViewDescriptor& descriptor)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Removes a consumer from the ViewInformation 'handle', and deletes it if
  // that was the last consumer.
  void RemoveConsumer(ViewInformation* handle) ABSL_LOCKS_EXCLUDED(mu_);

 private:
  // MeasureInformation stores all ViewInformation objects for a given measure.
  class MeasureInformation {
   public:
    explicit MeasureInformation(absl::Mutex* mu) : mu_(mu) {}

    // Merges measure_data into all views under this measure. Requires holding
    // *mu_;
    void MergeMeasureData(const opencensus::tags::TagMap& tags,
                          const MeasureData& data, absl::Time now);

    ViewInformation* AddConsumer(const ViewDescriptor& descriptor);
    void RemoveView(const ViewInformation* handle);

   private:
    absl::Mutex* const mu_;  // Not owned.
    // View objects hold a pointer to ViewInformation directly, so we do not
    // need fast lookup--lookup is only needed for view removal.
    std::vector<std::unique_ptr<ViewInformation>> views_ ABSL_GUARDED_BY(*mu_);
  };

  // TODO: PERF: Global synchronization is only needed for adding or
  // removing measures--we can reduce recording contention by claiming a reader
  // lock on mu_ and a writer lock on a measure-specific mutex.
  mutable absl::Mutex mu_;

  // All registered measures.
  std::vector<MeasureInformation> measures_ ABSL_GUARDED_BY(mu_);
};

extern template void StatsManager::AddMeasure(MeasureDouble measure);
extern template void StatsManager::AddMeasure(MeasureInt64 measure);

}  // namespace stats
}  // namespace opencensus

#endif  // OPENCENSUS_STATS_INTERNAL_STATS_MANAGER_H_
