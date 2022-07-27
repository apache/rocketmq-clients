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

#ifndef OPENCENSUS_TRACE_EXPORTER_ANNOTATION_H_
#define OPENCENSUS_TRACE_EXPORTER_ANNOTATION_H_

#include <string>
#include <unordered_map>

#include "absl/strings/string_view.h"
#include "opencensus/trace/exporter/attribute_value.h"

namespace opencensus {
namespace trace {
namespace exporter {

// An Annotation is an immutable object containing a text description, and
// optionally attributes. Annotations are used to trace operations for
// debugging. This class is immutable.
class Annotation final {
 public:
  explicit Annotation(
      absl::string_view description,
      std::unordered_map<std::string, AttributeValue> attributes =
          std::unordered_map<std::string, AttributeValue>())
      : description_(description), attributes_(std::move(attributes)) {}

  absl::string_view description() const { return description_; }

  const std::unordered_map<std::string, AttributeValue>& attributes() const {
    return attributes_;
  }

  // Returns a human-readable string for debugging. Do not rely on its format or
  // try to parse it.
  std::string DebugString() const;

 private:
  std::string description_;
  std::unordered_map<std::string, AttributeValue> attributes_;
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_EXPORTER_ANNOTATION_H_
