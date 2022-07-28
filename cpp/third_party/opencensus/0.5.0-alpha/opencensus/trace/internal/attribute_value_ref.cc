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

#include "opencensus/trace/attribute_value_ref.h"

#include <cassert>
#include <string>
#include <utility>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace opencensus {
namespace trace {

absl::string_view AttributeValueRef::string_value() const {
  assert(type() == Type::kString);
  return string_value_;
}

bool AttributeValueRef::bool_value() const {
  assert(type() == Type::kBool);
  return bool_value_;
}

int64_t AttributeValueRef::int_value() const {
  assert(type() == Type::kInt);
  return int_value_;
}

bool AttributeValueRef::operator==(const AttributeValueRef& v) const {
  if (type() != v.type()) {
    return false;
  }
  switch (v.type()) {
    case Type::kString:
      return string_value() == v.string_value();
    case Type::kBool:
      return bool_value() == v.bool_value();
    case Type::kInt:
      return int_value() == v.int_value();
  }
  return false;  // Unreachable.
}

bool AttributeValueRef::operator!=(const AttributeValueRef& v) const {
  return !(*this == v);
}

std::string AttributeValueRef::DebugString() const {
  switch (type()) {
    case Type::kString:
      return absl::StrCat("\"", string_value(), "\"");
    case Type::kBool:
      return bool_value() ? "true" : "false";
    case Type::kInt:
      return std::to_string(int_value());
  }
  return "";  // Unreachable.
}

}  // namespace trace
}  // namespace opencensus
