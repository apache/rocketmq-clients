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

#include "opencensus/tags/with_tag_map.h"

#include <iostream>
#include <thread>

#include "gtest/gtest.h"
#include "opencensus/context/context.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/tags/tag_map.h"

namespace opencensus {
namespace context {
class ContextTestPeer {
 public:
  static const opencensus::tags::TagMap& CurrentTags() {
    return Context::InternalMutableCurrent()->tags_;
  }
};
}  // namespace context
}  // namespace opencensus

// Not in namespace ::opencensus::context in order to better reflect what user
// code should look like.

namespace {

using opencensus::context::ContextTestPeer;

void LogCurrentContext() {
  const std::string s = opencensus::context::Context::Current().DebugString();
  std::cout << "  current: " << s << "\n";
}

void ExpectNoTags() {
  EXPECT_EQ(opencensus::tags::TagMap({}), ContextTestPeer::CurrentTags());
}

// Returns an example TagMap for testing.
opencensus::tags::TagMap Tags1() {
  static const auto k1 = opencensus::tags::TagKey::Register("key1");
  static const auto k2 = opencensus::tags::TagKey::Register("key2");
  return opencensus::tags::TagMap({{k1, "val1"}, {k2, "val2"}});
}

// Returns a different TagMap for testing.
opencensus::tags::TagMap Tags2() {
  static const auto k3 = opencensus::tags::TagKey::Register("key3");
  return opencensus::tags::TagMap({{k3, "val3"}});
}

TEST(WithTagsTest, NoTagsInDefaultContext) { ExpectNoTags(); }

TEST(WithTagsTest, TagsAreAddedAndRemoved) {
  const auto tags = Tags1();
  ExpectNoTags();
  {
    opencensus::tags::WithTagMap wt(tags);
    LogCurrentContext();
    EXPECT_EQ(tags, ContextTestPeer::CurrentTags()) << "Tags were installed.";
  }
  ExpectNoTags();
}

TEST(WithTagsTest, RvalueConstructor) {
  auto tags = Tags1();
  ExpectNoTags();
  {
    opencensus::tags::WithTagMap wt(std::move(tags));
    EXPECT_EQ(Tags1(), ContextTestPeer::CurrentTags())
        << "Tags were installed.";
  }
  ExpectNoTags();
}

TEST(WithTagsTest, Nesting) {
  const auto tags1 = Tags1();
  const auto tags2 = Tags2();

  ExpectNoTags();
  {
    opencensus::tags::WithTagMap wt1(tags1);
    EXPECT_EQ(tags1, ContextTestPeer::CurrentTags()) << "Tags1 installed.";
    {
      opencensus::tags::WithTagMap wt2(tags2);
      EXPECT_EQ(tags2, ContextTestPeer::CurrentTags()) << "Tags2 installed.";
    }
    EXPECT_EQ(tags1, ContextTestPeer::CurrentTags())
        << "Tags2 uninstalled, back to Tags1.";
  }
  ExpectNoTags();
}

TEST(WithTagsTest, DisabledViaConditional) {
  const auto tags1 = Tags1();
  const auto tags2 = Tags2();

  ExpectNoTags();
  {
    opencensus::tags::WithTagMap wt1(tags1);
    EXPECT_EQ(tags1, ContextTestPeer::CurrentTags()) << "Tags1 installed.";
    {
      opencensus::tags::WithTagMap wt2(tags2, false);
      EXPECT_EQ(tags1, ContextTestPeer::CurrentTags())
          << "Tags2 was NOT installed, blocked by condition.";
    }
    EXPECT_EQ(tags1, ContextTestPeer::CurrentTags())
        << "Still Tags1 after WithTags2 goes out of scope.";
  }
  ExpectNoTags();
}

#ifndef NDEBUG
TEST(WithTagsDeathTest, DestructorOnWrongThread) {
  const auto tags = Tags1();

  EXPECT_DEBUG_DEATH(
      {
        auto* wt = new opencensus::tags::WithTagMap(tags);
        std::thread t([wt]() {
          // Running the destructor in a different thread corrupts its
          // thread-local Context. In debug mode, it assert()s.
          delete wt;
        });
        t.join();
      },
      "must be destructed on the same thread");
}
#endif

}  // namespace
