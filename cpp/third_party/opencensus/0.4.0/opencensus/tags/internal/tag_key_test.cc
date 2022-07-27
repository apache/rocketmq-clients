// Copyright 2018, OpenCensus Authors
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

#include "opencensus/tags/tag_key.h"

#include "gtest/gtest.h"

namespace opencensus {
namespace tags {
namespace {

TEST(TagKeyTest, Name) {
  TagKey key = TagKey::Register("TestKey");
  EXPECT_EQ("TestKey", key.name());
}

TEST(TagKeyTest, DuplicateRegistrationsEqual) {
  TagKey k1 = TagKey::Register("key");
  TagKey k2 = TagKey::Register("key");
  EXPECT_EQ(k1, k2);
  EXPECT_EQ(k1.hash(), k2.hash());
}

TEST(TagKeyTest, Inequality) {
  TagKey k1 = TagKey::Register("k1");
  TagKey k2 = TagKey::Register("k2");
  EXPECT_NE(k1, k2);
  EXPECT_NE(k1.hash(), k2.hash());
}

}  // namespace
}  // namespace tags
}  // namespace opencensus
