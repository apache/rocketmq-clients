// Copyright 2019, OpenCensus Authors
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

#include "opencensus/tags/propagation/grpc_tags_bin.h"

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/tags/tag_map.h"

namespace opencensus {
namespace tags {
namespace propagation {
namespace {

TEST(GrpcTagsBinTest, DeserializeEmptyString) {
  TagMap m({});
  EXPECT_FALSE(FromGrpcTagsBinHeader("", &m));
}

TEST(GrpcTagsBinTest, DeserializeEmptyMap) {
  constexpr char tagsbin[] = {0};  // Just the version byte.
  TagMap m({});
  ASSERT_TRUE(
      FromGrpcTagsBinHeader(absl::string_view(tagsbin, sizeof(tagsbin)), &m));
  EXPECT_EQ(0, m.tags().size());
}

TEST(GrpcTagsBinTest, Deserialize) {
  constexpr char tagsbin[] = {
      0,                 // Version
      0,                 // Tag field
      3, 'k', 'e', 'y',  // k1
      3, 'v', 'a', 'l',  // v1
  };
  TagMap m({});
  ASSERT_TRUE(
      FromGrpcTagsBinHeader(absl::string_view(tagsbin, sizeof(tagsbin)), &m));
  ASSERT_EQ(1, m.tags().size());
  EXPECT_EQ("key", m.tags()[0].first.name());
  EXPECT_EQ("val", m.tags()[0].second);
}

TEST(GrpcTagsBinTest, DeserializeDupeKey) {
  constexpr char tagsbin[] = {
      0,                 // Version
      0,                 // Tag field
      3, 'k', 'e', 'y',  // k1
      2, 'v', '1',       // v1
      0,                 // Tag field
      3, 'k', 'e', 'y',  // k1
      2, 'v', '2',       // v2
  };
  TagMap m({});
  ASSERT_TRUE(
      FromGrpcTagsBinHeader(absl::string_view(tagsbin, sizeof(tagsbin)), &m));
  ASSERT_EQ(1, m.tags().size());
  EXPECT_EQ("key", m.tags()[0].first.name());
  EXPECT_EQ("v2", m.tags()[0].second);
}

TEST(GrpcTagsBinTest, DeserializeEmptyKey) {
  constexpr char tagsbin[] = {
      0,                 // Version
      0,                 // Tag field
      3, 'k', 'e', 'y',  // k1
      2, 'v', '1',       // v1
      0,                 // Tag field
      0,                 // k2
      2, 'v', '2',       // v2
  };
  TagMap m({});
  ASSERT_TRUE(
      FromGrpcTagsBinHeader(absl::string_view(tagsbin, sizeof(tagsbin)), &m));
  ASSERT_EQ(1, m.tags().size()) << "Empty key is dropped.";
  EXPECT_EQ("key", m.tags()[0].first.name());
  EXPECT_EQ("v1", m.tags()[0].second);
}

TEST(GrpcTagsBinTest, DeserializeEmptyValue) {
  constexpr char tagsbin[] = {
      0,                 // Version
      0,                 // Tag field
      3, 'k', 'e', 'y',  // k1
      0,                 // v1
  };
  TagMap m({});
  ASSERT_TRUE(
      FromGrpcTagsBinHeader(absl::string_view(tagsbin, sizeof(tagsbin)), &m));
  ASSERT_EQ(1, m.tags().size());
  EXPECT_EQ("key", m.tags()[0].first.name());
  EXPECT_TRUE(m.tags()[0].second.empty());
}

TEST(GrpcTagsBinTest, DeserializeTooShort) {
  constexpr char tagsbin[] = {
      0,  // Version
      0,  // Tag field
      3, 'k', 'e',
  };
  TagMap m({});
  EXPECT_FALSE(
      FromGrpcTagsBinHeader(absl::string_view(tagsbin, sizeof(tagsbin)), &m));
}

TEST(GrpcTagsBinTest, SerializeEmpty) {
  TagMap m({});
  constexpr char expected[] = {0};  // Just the version byte.
  EXPECT_EQ(absl::string_view(expected, sizeof(expected)),
            ToGrpcTagsBinHeader(m));
}

TEST(GrpcTagsBinTest, Serialize) {
  static const auto k1 = TagKey::Register("k1");
  static const auto k2 = TagKey::Register("key2");
  TagMap m({{k1, "v"}, {k2, "val"}});
  constexpr char expected[] = {
      0,                      // Version
      0,                      // Tag field
      2, 'k', '1',            // k1
      1, 'v',                 // v1
      0,                      // Tag field
      4, 'k', 'e', 'y', '2',  // k2
      3, 'v', 'a', 'l',       // v2
  };
  EXPECT_EQ(absl::string_view(expected, sizeof(expected)),
            ToGrpcTagsBinHeader(m));
}

TEST(GrpcTagsBinTest, SerializeLong) {
  TagMap m1({{TagKey::Register(std::string(300, 'A')), std::string(400, 'B')}});
  const std::string s = ToGrpcTagsBinHeader(m1);
  TagMap m2({});
  EXPECT_TRUE(FromGrpcTagsBinHeader(s, &m2));
  EXPECT_THAT(m1.tags(), ::testing::ContainerEq(m2.tags()));
}

TEST(GrpcTagsBinTest, SerializeTooLong) {
  std::vector<std::pair<opencensus::tags::TagKey, std::string>> tags;
  constexpr int kValLen = 20;
  int out_len = 1;
  int key_n = 0;
  while (out_len < 8192) {
    const std::string k = absl::StrCat("key_", key_n++);
    tags.emplace_back(TagKey::Register(k), std::string(kValLen, 'A'));
    out_len += 1 + 1 + k.size() + 1 + kValLen;
  }
  TagMap m(std::move(tags));
  EXPECT_EQ("", ToGrpcTagsBinHeader(m))
      << "Serialization failed due to value being too long.";
}

}  // namespace
}  // namespace propagation
}  // namespace tags
}  // namespace opencensus
