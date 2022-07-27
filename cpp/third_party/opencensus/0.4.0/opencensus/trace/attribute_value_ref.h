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

#ifndef OPENCENSUS_TRACE_ATTRIBUTE_VALUE_REF_H_
#define OPENCENSUS_TRACE_ATTRIBUTE_VALUE_REF_H_

#include <cstdint>
#include <initializer_list>
#include <string>
#include <type_traits>
#include <utility>

#include "absl/strings/string_view.h"

namespace opencensus {
namespace trace {

// AttributeValueRef represents a reference to the value of an Attribute. It can
// be one of the supported types: string, bool, or int64. In the case of string,
// AttributeValueRef holds an absl::string_view, and the caller must ensure that
// the backing storage outlives the AttributeValueRef object.
// AttributeValueRef is thread-compatible.
//
// When passing attrbutes to the tracing API, use an initializer list of
// AttributeValueRef. See AttributesRef in span.h.
class AttributeValueRef final {
 public:
  // The type of value held by this object.
  enum class Type : uint8_t { kString, kBool, kInt };

  // AttributeValueRef is copyable and movable.
  AttributeValueRef(const AttributeValueRef&) = default;
  AttributeValueRef(AttributeValueRef&&) = default;
  AttributeValueRef& operator=(const AttributeValueRef&) = default;
  AttributeValueRef& operator=(AttributeValueRef&&) = default;

  // We provide implicit constructors for string, int, and bool. We work around
  // the compiler coercing things like pointers and ints to bool.

  // Construct from absl::string_view.
  AttributeValueRef(absl::string_view string_value)
      : string_value_(string_value), type_(Type::kString) {}

  // Construct from C-style string.
  AttributeValueRef(const char* string_value)
      : string_value_(string_value), type_(Type::kString) {}

  // Construct from std::string.
  template <typename Allocator>
  AttributeValueRef(const std::basic_string<char, std::char_traits<char>,
                                            Allocator>& string_value)
      : string_value_(string_value), type_(Type::kString) {}

  // Construct from integer. We have to supply this form explicitly because
  // otherwise AttributeValueRef(int) is ambiguous between int and bool!
  template <typename T, typename std::enable_if<
                            std::is_integral<T>::value &&
                            !std::is_same<T, bool>::value>::type* = nullptr>
  AttributeValueRef(T int_value) : int_value_(int_value), type_(Type::kInt) {}

  // Construct from bool.
  template <typename T, typename std::enable_if<
                            std::is_same<T, bool>::value>::type* = nullptr>
  AttributeValueRef(T bool_value)
      : bool_value_(bool_value), type_(Type::kBool) {}

  // Accessors. The caller must ensure that the AttributeValueRef object is of
  // the corresponding type before accessing its value.
  Type type() const { return type_; }
  absl::string_view string_value() const;
  bool bool_value() const;
  int64_t int_value() const;

  // Equality of type and value.
  bool operator==(const AttributeValueRef& v) const;
  bool operator!=(const AttributeValueRef& v) const;

  // Returns a human-readable string for debugging. Do not rely on its format or
  // try to parse it.
  std::string DebugString() const;

 private:
  union {
    absl::string_view string_value_;
    int64_t int_value_;
    bool bool_value_;
  };
  Type type_;
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_ATTRIBUTE_VALUE_REF_H_
