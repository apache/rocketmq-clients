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

#include <iostream>

#include "gtest/gtest.h"
#include "opencensus/trace/exporter/attribute_value.h"

namespace opencensus {
namespace trace {
namespace exporter {
namespace {

TEST(AnnotationTest, Description) {
  Annotation a("hello");
  EXPECT_EQ("hello", a.description());
}

Annotation MakeAnnotation() {
  return Annotation("This is an annotation.",
                    {
                        {"hello", AttributeValue("world")},
                        {"latency", AttributeValue(1234)},
                        {"bool", AttributeValue(true)},
                    });
}

TEST(AnnotationTest, Attributes) {
  auto a = MakeAnnotation();
  EXPECT_EQ(3, a.attributes().size());
}

TEST(AnnotationTest, DebugStringIsNotEmpty) {
  auto a = MakeAnnotation();
  const std::string s = a.DebugString();
  std::cout << s << "\n";
  EXPECT_NE("", s);
}

}  // namespace
}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
