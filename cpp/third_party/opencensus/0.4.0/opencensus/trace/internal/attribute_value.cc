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

#include "opencensus/trace/exporter/attribute_value.h"

#include <cassert>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "opencensus/trace/attribute_value_ref.h"

namespace opencensus {
namespace trace {
namespace exporter {

AttributeValue::AttributeValue(AttributeValueRef ref) : type_(ref.type()) {
  switch (type_) {
    case Type::kString:
      new (&string_value_) std::string(ref.string_value());
      break;
    case Type::kBool:
      bool_value_ = ref.bool_value();
      break;
    case Type::kInt:
      int_value_ = ref.int_value();
      break;
  }
}

AttributeValue::AttributeValue(const AttributeValue& v) : type_(v.type()) {
  switch (type_) {
    case Type::kString:
      new (&string_value_) std::string(v.string_value());
      break;
    case Type::kBool:
      bool_value_ = v.bool_value();
      break;
    case Type::kInt:
      int_value_ = v.int_value();
      break;
  }
}

AttributeValue::AttributeValue(AttributeValue&& v) : type_(v.type()) {
  switch (type_) {
    case Type::kString:
      new (&string_value_) std::string(std::move(v.string_value_));
      break;
    case Type::kBool:
      bool_value_ = v.bool_value();
      break;
    case Type::kInt:
      int_value_ = v.int_value();
      break;
  }
}

AttributeValue& AttributeValue::operator=(const AttributeValue& v) {
  this->~AttributeValue();
  type_ = v.type_;
  switch (type_) {
    case Type::kString:
      new (&string_value_) std::string(v.string_value_);
      break;
    case Type::kBool:
      bool_value_ = v.bool_value();
      break;
    case Type::kInt:
      int_value_ = v.int_value();
      break;
  }
  return *this;
}

AttributeValue& AttributeValue::operator=(AttributeValue&& v) {
  this->~AttributeValue();
  type_ = v.type_;
  switch (type_) {
    case Type::kString:
      new (&string_value_) std::string(std::move(v.string_value_));
      break;
    case Type::kBool:
      bool_value_ = v.bool_value();
      break;
    case Type::kInt:
      int_value_ = v.int_value();
      break;
  }
  return *this;
}

AttributeValue::~AttributeValue() {
  if (type_ == Type::kString) {
    using std::string;
    string_value_.~string();
  }
}

const std::string& AttributeValue::string_value() const {
  assert(type() == Type::kString);
  return string_value_;
}

bool AttributeValue::bool_value() const {
  assert(type() == Type::kBool);
  return bool_value_;
}

int64_t AttributeValue::int_value() const {
  assert(type() == Type::kInt);
  return int_value_;
}

bool AttributeValue::operator==(const AttributeValue& v) const {
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

bool AttributeValue::operator!=(const AttributeValue& v) const {
  return !(*this == v);
}

std::string AttributeValue::DebugString() const {
  switch (type()) {
    case Type::kString:
      return "\"" + string_value() + "\"";
    case Type::kBool:
      return bool_value() ? "true" : "false";
    case Type::kInt:
      return std::to_string(int_value());
  }
  return "";  // Unreachable.
}

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
