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

#ifndef OPENCENSUS_STATS_TESTING_TEST_UTILS_H_
#define OPENCENSUS_STATS_TESTING_TEST_UTILS_H_

#include <initializer_list>
#include <string>
#include <utility>
#include <vector>

#include "opencensus/stats/bucket_boundaries.h"
#include "opencensus/stats/distribution.h"
#include "opencensus/stats/internal/view_data_impl.h"
#include "opencensus/stats/view_data.h"

namespace opencensus {
namespace stats {
namespace testing {

// Struct to be used as parameters to MakeViewData functions.
struct TestViewValue {
  std::vector<std::string> tag_values;
  double value;
  absl::Time start_time;
};

class TestUtils final {
 public:
  // Makes a ViewData, using absl::UnixEpoch() as the start time.
  static ViewData MakeViewData(
      const ViewDescriptor& descriptor,
      std::initializer_list<std::pair<std::vector<std::string>, double>>
          values);

  // Makes a ViewData, using the specified start times for each timeseries.
  static ViewData MakeViewDataWithStartTimes(
      const ViewDescriptor& descriptor,
      const std::vector<TestViewValue>& view_values);

  static Distribution MakeDistribution(const BucketBoundaries* buckets);

  static void AddToDistribution(Distribution* distribution, double value);

  // Flushes the DeltaProducer, propagating recorded stats to views.
  static void Flush();

  TestUtils() = delete;
};

}  // namespace testing
}  // namespace stats
}  // namespace opencensus

#endif  // OPENCENSUS_STATS_TESTING_TEST_UTILS_H_
