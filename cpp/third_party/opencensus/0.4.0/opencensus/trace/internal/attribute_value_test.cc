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

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"
#include "opencensus/trace/attribute_value_ref.h"

namespace opencensus {
namespace trace {
namespace exporter {
namespace {

TEST(AttributeValueTest, StringValue) {
  AttributeValue attr(absl::StrCat("hello", " ", "world"));
  EXPECT_EQ(AttributeValue::Type::kString, attr.type());
  EXPECT_EQ("hello world", attr.string_value());
}

TEST(AttributeValueTest, StringLiteralValue) {
  AttributeValue attr("hello");
  EXPECT_EQ(AttributeValue::Type::kString, attr.type());
  EXPECT_EQ("hello", attr.string_value());

  const char* s1 = "hello world";
  AttributeValue a1(s1);
  EXPECT_EQ(AttributeValue::Type::kString, a1.type());

  constexpr char s2[] = "foobar";
  AttributeValue a2(s2);
  EXPECT_EQ(AttributeValue::Type::kString, a2.type());
}

TEST(AttributeValueTest, BoolValue) {
  AttributeValue attr(true);
  EXPECT_EQ(AttributeValue::Type::kBool, attr.type());
  EXPECT_TRUE(attr.bool_value());

  attr = AttributeValue(false);
  EXPECT_EQ(AttributeValue::Type::kBool, attr.type());
  EXPECT_FALSE(attr.bool_value());
}

TEST(AttributeValueTest, IntValue) {
  AttributeValue attr(123);
  EXPECT_EQ(AttributeValue::Type::kInt, attr.type());
  EXPECT_EQ(123, attr.int_value());

  char c = 'A';
  AttributeValue a3(c);
  EXPECT_EQ(AttributeValue::Type::kInt, a3.type()) << "Not a string!";
}

TEST(AttributeValueTest, Equality) {
  {
    AttributeValue a("hello");
    AttributeValue b("hello");
    EXPECT_TRUE(a == b);
    EXPECT_FALSE(a != b);
    // Mismatch value.
    b = AttributeValue("hi");
    EXPECT_TRUE(a != b);
    EXPECT_FALSE(a == b);
    // Copy assignment and match value again.
    b = a;
    EXPECT_TRUE(a == b);
    EXPECT_FALSE(a != b);
  }
  {
    AttributeValue a(true);
    AttributeValue b(true);
    EXPECT_TRUE(a == b);
    EXPECT_FALSE(a != b);
    // Mismatch value.
    b = AttributeValue(false);
    EXPECT_TRUE(a != b);
    EXPECT_FALSE(a == b);
    // Mismatch type.
    b = AttributeValue(123);
    EXPECT_TRUE(a != b);
    EXPECT_FALSE(a == b);
  }
  {
    AttributeValue a(123);
    AttributeValue b(123);
    EXPECT_TRUE(a == b);
    EXPECT_FALSE(a != b);
    // Mismatch value.
    b = AttributeValue(456);
    EXPECT_TRUE(a != b);
    EXPECT_FALSE(a == b);
  }
}

TEST(AttributeValueTest, DebugStringIsNotEmpty) {
  AttributeValue attr("hello");
  EXPECT_NE("", attr.DebugString());
}

}  // namespace
}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
