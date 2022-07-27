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

#include "opencensus/stats/testing/test_utils.h"

#include <initializer_list>
#include <memory>
#include <string>
#include <utility>

#include "absl/memory/memory.h"
#include "absl/time/time.h"
#include "opencensus/stats/bucket_boundaries.h"
#include "opencensus/stats/internal/delta_producer.h"
#include "opencensus/stats/internal/measure_data.h"

namespace opencensus {
namespace stats {
namespace testing {

// static
ViewData TestUtils::MakeViewData(
    const ViewDescriptor& descriptor,
    std::initializer_list<std::pair<std::vector<std::string>, double>> values) {
  auto impl = absl::make_unique<ViewDataImpl>(absl::UnixEpoch(), descriptor);
  std::vector<BucketBoundaries> boundaries = {
      descriptor.aggregation().bucket_boundaries()};
  for (const auto& value : values) {
    MeasureData measure_data = MeasureData(boundaries);
    measure_data.Add(value.second);
    impl->Merge(value.first, measure_data, absl::UnixEpoch());
  }
  if (impl->type() == ViewDataImpl::Type::kStatsObject) {
    return ViewData(absl::make_unique<ViewDataImpl>(*impl, absl::UnixEpoch()));
  } else {
    return ViewData(std::move(impl));
  }
}

// static
Distribution TestUtils::MakeDistribution(const BucketBoundaries* buckets) {
  return Distribution(buckets);
}

// static
void TestUtils::AddToDistribution(Distribution* distribution, double value) {
  distribution->Add(value);
}

// static
void TestUtils::Flush() { DeltaProducer::Get()->Flush(); }

}  // namespace testing
}  // namespace stats
}  // namespace opencensus
