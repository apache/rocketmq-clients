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

#ifndef OPENCENSUS_COMMON_INTERNAL_TIMESTAMP_H_
#define OPENCENSUS_COMMON_INTERNAL_TIMESTAMP_H_

#include "absl/time/time.h"
#include "google/protobuf/timestamp.pb.h"

namespace opencensus {
namespace common {

// Converts time to proto.
void SetTimestamp(absl::Time time, google::protobuf::Timestamp* proto);

}  // namespace common
}  // namespace opencensus

#endif  // OPENCENSUS_COMMON_INTERNAL_TIMESTAMP_H_
