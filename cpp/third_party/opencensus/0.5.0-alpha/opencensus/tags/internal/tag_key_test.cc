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

#include "absl/hash/hash.h"
#include "absl/strings/str_cat.h"
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
  EXPECT_EQ(absl::Hash<TagKey>()(k1), absl::Hash<TagKey>()(k2));
}

TEST(TagKeyTest, Inequality) {
  TagKey k1 = TagKey::Register("k1");
  TagKey k2 = TagKey::Register("k2");
  EXPECT_NE(k1, k2);
  EXPECT_NE(absl::Hash<TagKey>()(k1), absl::Hash<TagKey>()(k2));
}

// Test that the reference returned by TagKey::name() isn't invalidated when the
// registry's underlying storage grows and moves.
TEST(TagKeyTest, GrowRegistry) {
  TagKey k = TagKey::Register("my_key");
  const std::string& s = k.name();
  ASSERT_EQ("my_key", s);
  for (int i = 0; i < 1000; ++i) {
    TagKey::Register(absl::StrCat("key_", i));
    EXPECT_EQ("my_key", s) << "iteration " << i;
  }
}

}  // namespace
}  // namespace tags
}  // namespace opencensus
