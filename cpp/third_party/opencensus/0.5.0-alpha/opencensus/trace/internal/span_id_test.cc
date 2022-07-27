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

#include "opencensus/trace/span_id.h"

#include "gtest/gtest.h"

namespace opencensus {
namespace trace {
namespace {

constexpr uint8_t span_id[] = {1, 2, 3, 4, 5, 6, 7, 8};

TEST(SpanIdTest, Equality) {
  SpanId id1;
  SpanId id2(span_id);
  SpanId id3(span_id);

  EXPECT_FALSE(id1 == id2);
  EXPECT_EQ(id2, id3);
}

TEST(SpanIdTest, IsValid) {
  SpanId id1;
  SpanId id2(span_id);

  EXPECT_FALSE(id1.IsValid());
  EXPECT_TRUE(id2.IsValid());
}

TEST(SpanIdTest, ToHex) {
  SpanId id(span_id);
  EXPECT_EQ("0102030405060708", id.ToHex());
}

}  // namespace
}  // namespace trace
}  // namespace opencensus
