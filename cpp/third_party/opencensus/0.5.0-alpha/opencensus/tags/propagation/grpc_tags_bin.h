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

#ifndef OPENCENSUS_TAGS_PROPAGATION_GRPC_TAGS_BIN_H_
#define OPENCENSUS_TAGS_PROPAGATION_GRPC_TAGS_BIN_H_

#include <string>

#include "absl/strings/string_view.h"
#include "opencensus/tags/tag_map.h"

namespace opencensus {
namespace tags {
namespace propagation {

// Parses the value of the binary grpc-tags-bin header, populating a
// TagMap. Returns false if parsing fails.
//
// See also:
// https://github.com/census-instrumentation/opencensus-specs/blob/master/encodings/BinaryEncoding.md
bool FromGrpcTagsBinHeader(absl::string_view header, TagMap* out);

// Returns a value for the grpc-tags-bin header, or the empty string if
// serialization failed.
std::string ToGrpcTagsBinHeader(const TagMap& tags);

}  // namespace propagation
}  // namespace tags
}  // namespace opencensus

#endif  // OPENCENSUS_TAGS_PROPAGATION_GRPC_TAGS_BIN_H_
