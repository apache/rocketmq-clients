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

#include "absl/strings/str_cat.h"
#include "gtest/gtest.h"

namespace opencensus {
namespace trace {
namespace {

TEST(AttributeValueRefTest, StringValue) {
  // String has to outlive AttributeValueRef.
  const std::string s = absl::StrCat("hello", " ", "world");
  AttributeValueRef attr(s);
  EXPECT_EQ(AttributeValueRef::Type::kString, attr.type());
  EXPECT_EQ("hello world", attr.string_value());
}

TEST(AttributeValueRefTest, StringLiteralValue) {
  AttributeValueRef attr("hello");
  EXPECT_EQ(AttributeValueRef::Type::kString, attr.type());
  EXPECT_EQ("hello", attr.string_value());

  const char* s1 = "hello world";
  AttributeValueRef a1(s1);
  EXPECT_EQ(AttributeValueRef::Type::kString, a1.type());

  constexpr char s2[] = "foobar";
  AttributeValueRef a2(s2);
  EXPECT_EQ(AttributeValueRef::Type::kString, a2.type());
}

TEST(AttributeValueRefTest, BoolValue) {
  AttributeValueRef attr(true);
  EXPECT_EQ(AttributeValueRef::Type::kBool, attr.type());
  EXPECT_TRUE(attr.bool_value());

  attr = false;
  EXPECT_EQ(AttributeValueRef::Type::kBool, attr.type());
  EXPECT_FALSE(attr.bool_value());
}

TEST(AttributeValueRefTest, IntValue) {
  AttributeValueRef attr(123);
  EXPECT_EQ(AttributeValueRef::Type::kInt, attr.type());
  EXPECT_EQ(123, attr.int_value());

  char c = 'A';
  AttributeValueRef a3(c);
  EXPECT_EQ(AttributeValueRef::Type::kInt, a3.type()) << "Not a string!";
}

TEST(AttributeValueRefTest, Equality) {
  {
    AttributeValueRef a("hello");
    AttributeValueRef b("hello");
    EXPECT_TRUE(a == b);
    EXPECT_FALSE(a != b);
    // Mismatch value.
    b = "hi";
    EXPECT_TRUE(a != b);
    EXPECT_FALSE(a == b);
  }
  {
    AttributeValueRef a(true);
    AttributeValueRef b(true);
    EXPECT_TRUE(a == b);
    EXPECT_FALSE(a != b);
    // Mismatch value.
    b = false;
    EXPECT_TRUE(a != b);
    EXPECT_FALSE(a == b);
    // Mismatch type.
    b = 123;
    EXPECT_TRUE(a != b);
    EXPECT_FALSE(a == b);
  }
  {
    AttributeValueRef a(123);
    AttributeValueRef b(123);
    EXPECT_TRUE(a == b);
    EXPECT_FALSE(a != b);
    // Mismatch value.
    b = 456;
    EXPECT_TRUE(a != b);
    EXPECT_FALSE(a == b);
  }
}

TEST(AttributeValueRefTest, DebugStringIsNotEmpty) {
  AttributeValueRef attr("hello");
  EXPECT_NE("", attr.DebugString());
  std::cout << attr.DebugString() << "\n";
}

TEST(AttributeValueRefTest, EveryConstructorWorks) {
  AttributeValueRef attr("initial value");
  attr = 1;
  std::cout << attr.DebugString() << "\n";
  attr = 2;
  std::cout << attr.DebugString() << "\n";
  attr = true;
  std::cout << attr.DebugString() << "\n";
  attr = false;
  std::cout << attr.DebugString() << "\n";
  attr = "hello again";
  std::cout << attr.DebugString() << "\n";
}

}  // namespace
}  // namespace trace
}  // namespace opencensus
