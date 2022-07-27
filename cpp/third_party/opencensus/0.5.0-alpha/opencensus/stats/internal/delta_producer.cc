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

#include "opencensus/stats/internal/delta_producer.h"

#include <algorithm>
#include <cstdint>
#include <memory>
#include <vector>

#include "absl/synchronization/mutex.h"
#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "opencensus/stats/bucket_boundaries.h"
#include "opencensus/stats/internal/measure_data.h"
#include "opencensus/stats/internal/measure_registry_impl.h"
#include "opencensus/stats/internal/stats_manager.h"

namespace opencensus {
namespace stats {

void Delta::Record(std::initializer_list<Measurement> measurements,
                   opencensus::tags::TagMap tags) {
  auto it = delta_.find(tags);
  if (it == delta_.end()) {
    it = delta_.emplace_hint(it, std::piecewise_construct,
                             std::make_tuple(std::move(tags)),
                             std::make_tuple(std::vector<MeasureData>()));
    it->second.reserve(registered_boundaries_.size());
    for (const auto& boundaries_for_measure : registered_boundaries_) {
      it->second.emplace_back(boundaries_for_measure);
    }
  }
  for (const auto& measurement : measurements) {
    const uint64_t index = MeasureRegistryImpl::IdToIndex(measurement.id_);
    ABSL_ASSERT(index < registered_boundaries_.size());
    switch (MeasureRegistryImpl::IdToType(measurement.id_)) {
      case MeasureDescriptor::Type::kDouble:
        it->second[index].Add(measurement.value_double_);
        break;
      case MeasureDescriptor::Type::kInt64:
        it->second[index].Add(measurement.value_int_);
        break;
    }
  }
}

void Delta::clear() {
  registered_boundaries_.clear();
  delta_.clear();
}

void Delta::SwapAndReset(
    std::vector<std::vector<BucketBoundaries>>& registered_boundaries,
    Delta* other) {
  registered_boundaries_.swap(other->registered_boundaries_);
  delta_.swap(other->delta_);
  delta_.clear();
  registered_boundaries_ = registered_boundaries;
}

DeltaProducer* DeltaProducer::Get() {
  static DeltaProducer* global_delta_producer = new DeltaProducer;
  return global_delta_producer;
}

void DeltaProducer::AddMeasure() {
  delta_mu_.Lock();
  absl::MutexLock harvester_lock(&harvester_mu_);
  registered_boundaries_.push_back({});
  SwapDeltas();
  delta_mu_.Unlock();
  ConsumeLastDelta();
}

void DeltaProducer::AddBoundaries(uint64_t index,
                                  const BucketBoundaries& boundaries) {
  delta_mu_.Lock();
  auto& measure_boundaries = registered_boundaries_[index];
  if (std::find(measure_boundaries.begin(), measure_boundaries.end(),
                boundaries) == measure_boundaries.end()) {
    absl::MutexLock harvester_lock(&harvester_mu_);
    measure_boundaries.push_back(boundaries);
    SwapDeltas();
    delta_mu_.Unlock();
    ConsumeLastDelta();
  } else {
    delta_mu_.Unlock();
  }
}

void DeltaProducer::Record(std::initializer_list<Measurement> measurements,
                           opencensus::tags::TagMap tags) {
  absl::MutexLock l(&delta_mu_);
  active_delta_.Record(measurements, std::move(tags));
}

void DeltaProducer::Flush() {
  delta_mu_.Lock();
  absl::MutexLock harvester_lock(&harvester_mu_);
  SwapDeltas();
  delta_mu_.Unlock();
  ConsumeLastDelta();
}

DeltaProducer::DeltaProducer()
    : harvester_thread_(&DeltaProducer::RunHarvesterLoop, this) {}

void DeltaProducer::SwapDeltas() {
  ABSL_ASSERT(last_delta_.delta().empty() && "Last delta was not consumed.");
  active_delta_.SwapAndReset(registered_boundaries_, &last_delta_);
}

void DeltaProducer::ConsumeLastDelta() {
  StatsManager::Get()->MergeDelta(last_delta_);
  last_delta_.clear();
}

void DeltaProducer::RunHarvesterLoop() {
  absl::Time next_harvest_time = absl::Now() + harvest_interval_;
  while (true) {
    const absl::Time now = absl::Now();
    absl::SleepFor(next_harvest_time - now);
    // Account for the possibility that the last harvest took longer than
    // harvest_interval_ and we are already past next_harvest_time.
    next_harvest_time = std::max(next_harvest_time, now) + harvest_interval_;
    Flush();
  }
}

}  // namespace stats
}  // namespace opencensus
