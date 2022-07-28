// Copyright 2019, OpenCensus Authors
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

#include "opencensus/common/internal/timestamp.h"

#include <cstdint>

#include "absl/time/time.h"
#include "google/protobuf/timestamp.pb.h"

namespace opencensus {
namespace common {

void SetTimestamp(absl::Time time, google::protobuf::Timestamp* proto) {
  const int64_t seconds = absl::ToUnixSeconds(time);
  proto->set_seconds(seconds);
  proto->set_nanos(
      absl::ToInt64Nanoseconds(time - absl::FromUnixSeconds(seconds)));
}

}  // namespace common
}  // namespace opencensus
