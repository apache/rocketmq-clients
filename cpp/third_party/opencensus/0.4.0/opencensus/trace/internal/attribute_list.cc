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

#include "opencensus/trace/internal/attribute_list.h"

#include <utility>

#include "absl/strings/string_view.h"

namespace opencensus {
namespace trace {

uint32_t AttributeList::num_attributes_dropped() const {
  return total_recorded_attributes_ - attributes_.size();
}

uint32_t AttributeList::num_attributes_added() const {
  return total_recorded_attributes_;
}

void AttributeList::AddAttribute(absl::string_view key,
                                 exporter::AttributeValue&& value) {
  // Blank span has 0 max attributes.
  if (max_attributes_ == 0) {
    return;
  }

  std::string key_string(key);
  auto it = attributes_.find(key_string);
  if (it != attributes_.end()) {
    it->second = std::move(value);
    return;
  }

  if (attributes_.size() >= max_attributes_) {
    // TODO: This should be changed to a LRU mechanism.  Just remove
    // first element for now.
    attributes_.erase(attributes_.begin());
  }
  attributes_.insert({key_string, std::move(value)});
  total_recorded_attributes_++;
}

}  // namespace trace
}  // namespace opencensus
