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

#include "opencensus/trace/exporter/annotation.h"

#include <string>
#include <utility>

#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "opencensus/trace/exporter/attribute_value.h"

namespace opencensus {
namespace trace {
namespace exporter {

std::string Annotation::DebugString() const {
  using std::string;
  string out = description_;
  if (!attributes_.empty()) {
    absl::StrAppend(
        &out, " (attributes: ",
        absl::StrJoin(
            attributes_, ", ",
            [](string* o,
               std::pair<const std::string&, const AttributeValue&> kv) {
              absl::StrAppend(o, "\"", kv.first,
                              "\":", kv.second.DebugString());
            }),
        ")");
  }
  return out;
}

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
