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

// IWYU pragma: private, include "opencensus/stats/stats.h"
// IWYU pragma: friend opencensus/stats/.*

#ifndef OPENCENSUS_STATS_TAG_SET_H_
#define OPENCENSUS_STATS_TAG_SET_H_

#include "absl/base/macros.h"
#include "opencensus/tags/tag_map.h"

namespace opencensus {
namespace stats {

ABSL_DEPRECATED(
    "TagSet has moved to opencensus::tags::TagMap. This is a "
    "compatibility shim and will be removed on or after 2019-03-20")
typedef opencensus::tags::TagMap TagSet;

}  // namespace stats
}  // namespace opencensus

#endif  // OPENCENSUS_STATS_TAG_SET_H_
