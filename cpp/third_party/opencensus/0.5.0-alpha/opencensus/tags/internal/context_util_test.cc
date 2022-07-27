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

#include "opencensus/tags/context_util.h"

#include "gtest/gtest.h"
#include "opencensus/context/context.h"
#include "opencensus/tags/tag_key.h"
#include "opencensus/tags/tag_map.h"
#include "opencensus/tags/with_tag_map.h"

// Not in namespace ::opencensus::context in order to better reflect what user
// code should look like.

namespace {

// Returns an example TagMap for testing.
opencensus::tags::TagMap Tags1() {
  static const auto k1 = opencensus::tags::TagKey::Register("key1");
  static const auto k2 = opencensus::tags::TagKey::Register("key2");
  return opencensus::tags::TagMap({{k1, "val1"}, {k2, "val2"}});
}

TEST(FromContextTest, GetCurrentTagMap) {
  EXPECT_TRUE(opencensus::tags::GetCurrentTagMap().tags().empty());
  {
    opencensus::tags::WithTagMap wt(Tags1());
    EXPECT_EQ(Tags1().tags(), opencensus::tags::GetCurrentTagMap().tags());
  }
}

TEST(FromContextTest, GetTagMapFromContext) {
  opencensus::context::Context ctx = opencensus::context::Context::Current();
  EXPECT_TRUE(opencensus::tags::GetTagMapFromContext(ctx).tags().empty());
  {
    opencensus::tags::WithTagMap wt(Tags1());
    ctx = opencensus::context::Context::Current();
  }
  EXPECT_EQ(Tags1().tags(), opencensus::tags::GetTagMapFromContext(ctx).tags());
}

}  // namespace
