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

#ifndef OPENCENSUS_TRACE_INTERNAL_ATTRIBUTE_LIST_H_
#define OPENCENSUS_TRACE_INTERNAL_ATTRIBUTE_LIST_H_

#include <cstdint>
#include <string>
#include <unordered_map>

#include "absl/strings/string_view.h"
#include "opencensus/trace/exporter/attribute_value.h"

namespace opencensus {
namespace trace {

// Stores a list of AttributesValues that are recorded within a span. The
// AttributeValues are stored in an unordered fashion and accessed with a
// string key. AttributeList is thread-compatible.
class AttributeList final {
 public:
  explicit AttributeList(uint32_t max_attributes = 0)
      : total_recorded_attributes_(0), max_attributes_(max_attributes) {}

  // Returns the number of the dropped attributes.
  uint32_t num_attributes_dropped() const;

  // Returns the number of recorded attributes, including dropped attributes.
  uint32_t num_attributes_added() const;

  // Adds an AttributeValue to the list or updates an existing AttributeValue.
  // If max_attributes_ is exceeded, it will evict one of the previous
  // AttributeValues.
  void AddAttribute(absl::string_view key, exporter::AttributeValue&& value);

  // Returns an unordered map of all the attributes that are currently contained
  // within the list.
  const std::unordered_map<std::string, exporter::AttributeValue>& attributes()
      const {
    return attributes_;
  }

 private:
  uint32_t total_recorded_attributes_;
  const uint32_t max_attributes_;
  std::unordered_map<std::string, exporter::AttributeValue> attributes_;
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_INTERNAL_ATTRIBUTE_LIST_H_
