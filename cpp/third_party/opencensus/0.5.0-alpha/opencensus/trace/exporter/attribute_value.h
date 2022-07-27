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

#ifndef OPENCENSUS_TRACE_EXPORTER_ATTRIBUTE_VALUE_H_
#define OPENCENSUS_TRACE_EXPORTER_ATTRIBUTE_VALUE_H_

#include <cstdint>
#include <initializer_list>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/strings/string_view.h"
#include "opencensus/trace/attribute_value_ref.h"

namespace opencensus {
namespace trace {
namespace exporter {

// AttributeValue stores the value of an Attribute. It can be one of the
// supported types: string, bool, or int64. AttributeValue is thread-compatible.
//
// When passing attributes to the tracing API, use the cheaper
// AttributeValueRef.
class AttributeValue final {
 public:
  // The type of value held by this object.
  using Type = AttributeValueRef::Type;

  explicit AttributeValue(AttributeValueRef ref);
  ~AttributeValue();

  // AttributeValue is copyable and movable.
  AttributeValue(const AttributeValue&);
  AttributeValue(AttributeValue&&);
  AttributeValue& operator=(const AttributeValue&);
  AttributeValue& operator=(AttributeValue&&);

  // Accessors. The caller must ensure that the AttributeValue object is of the
  // corresponding type before accessing its value.
  Type type() const { return type_; }
  const std::string& string_value() const;
  bool bool_value() const;
  int64_t int_value() const;

  // Equality of type and value.
  bool operator==(const AttributeValue& v) const;
  bool operator!=(const AttributeValue& v) const;

  // Returns a human-readable string for debugging. Do not rely on its format or
  // try to parse it.
  std::string DebugString() const;

 private:
  union {
    std::string string_value_;
    int64_t int_value_;
    bool bool_value_;
  };
  // type_ is last to minimize padding.
  Type type_;
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_EXPORTER_ATTRIBUTE_VALUE_H_
