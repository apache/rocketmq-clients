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

#include "opencensus/tags/tag_map.h"

#include <iostream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "gmock/gmock.h"
#include "gtest/gtest.h"

namespace opencensus {
namespace tags {
namespace {

using ::testing::HasSubstr;

TEST(TagMapTest, ConstructorsEquivalent) {
  TagKey key = TagKey::Register("k");
  const std::vector<std::pair<TagKey, std::string>> tags({{key, "v"}});
  EXPECT_EQ(TagMap(tags), TagMap({{key, "v"}}));
}

TEST(TagMapTest, TagsSorted) {
  TagKey k1 = TagKey::Register("b");
  TagKey k2 = TagKey::Register("c");
  TagKey k3 = TagKey::Register("a");
  const std::vector<std::pair<TagKey, std::string>> expected = {
      {k1, "v"}, {k2, "v"}, {k3, "v"}};
  const std::vector<std::pair<TagKey, std::string>> tags(
      {{k2, "v"}, {k3, "v"}, {k1, "v"}});
  EXPECT_THAT(TagMap(tags).tags(), ::testing::ElementsAreArray(expected));
  EXPECT_THAT(TagMap({{k2, "v"}, {k1, "v"}, {k3, "v"}}).tags(),
              ::testing::ElementsAreArray(expected));
}

TEST(TagMapTest, EqualityDisregardsOrder) {
  TagKey k1 = TagKey::Register("k1");
  TagKey k2 = TagKey::Register("k2");
  EXPECT_EQ(TagMap({{k1, "v1"}, {k2, "v2"}}), TagMap({{k2, "v2"}, {k1, "v1"}}));
}

TEST(TagMapTest, EqualityRespectsMissingKeys) {
  TagKey k1 = TagKey::Register("k1");
  TagKey k2 = TagKey::Register("k2");
  EXPECT_NE(TagMap({{k1, "v1"}, {k2, "v2"}}), TagMap({{k1, "v1"}}));
}

TEST(TagMapTest, EqualityRespectsKeyValuePairings) {
  TagKey k1 = TagKey::Register("k1");
  TagKey k2 = TagKey::Register("k2");
  EXPECT_NE(TagMap({{k1, "v1"}, {k2, "v2"}}), TagMap({{k1, "v2"}, {k2, "v1"}}));
}

TEST(TagMapTest, HashDisregardsOrder) {
  TagKey k1 = TagKey::Register("k1");
  TagKey k2 = TagKey::Register("k2");
  TagMap ts1({{k1, "v1"}, {k2, "v2"}});
  TagMap ts2({{k2, "v2"}, {k1, "v1"}});
  EXPECT_EQ(TagMap::Hash()(ts1), TagMap::Hash()(ts2));
}

TEST(TagMapTest, HashRespectsKeyValuePairings) {
  TagKey k1 = TagKey::Register("k1");
  TagKey k2 = TagKey::Register("k2");
  TagMap ts1({{k1, "v1"}, {k2, "v2"}});
  TagMap ts2({{k1, "v2"}, {k2, "v1"}});
  EXPECT_NE(TagMap::Hash()(ts1), TagMap::Hash()(ts2));
}

TEST(TagMapTest, UnorderedMap) {
  // Test that the operators and hash are compatible with std::unordered_map.
  TagKey key = TagKey::Register("key");
  std::unordered_map<TagMap, int, TagMap::Hash> map;
  TagMap ts = {{key, "value"}};
  map.emplace(ts, 1);
  EXPECT_NE(map.end(), map.find(ts));
  EXPECT_EQ(1, map.erase(ts));
}

TEST(TagMapTest, DebugStringContainsTags) {
  TagKey k1 = TagKey::Register("key1");
  TagKey k2 = TagKey::Register("key2");
  TagMap ts({{k1, "value1"}, {k2, "value2"}});
  const std::string s = ts.DebugString();
  std::cout << s << "\n";
  EXPECT_THAT(s, HasSubstr("key1"));
  EXPECT_THAT(s, HasSubstr("value1"));
  EXPECT_THAT(s, HasSubstr("key2"));
  EXPECT_THAT(s, HasSubstr("value2"));
}

TEST(TagMapDeathTest, DuplicateKeysNotAllowed) {
  TagKey k = TagKey::Register("k");
  EXPECT_DEBUG_DEATH(
      {
        TagMap m({{k, "v1"}, {k, "v2"}});
      },
      "Duplicate keys are not allowed");
}

}  // namespace
}  // namespace tags
}  // namespace opencensus
