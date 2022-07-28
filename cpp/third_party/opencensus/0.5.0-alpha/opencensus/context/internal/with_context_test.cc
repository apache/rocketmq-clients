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

#include "opencensus/context/with_context.h"

#include <thread>
#include <utility>

#include "gtest/gtest.h"
#include "opencensus/context/context.h"

// Not in namespace ::opencensus::context in order to better reflect what user
// code should look like.

namespace {

TEST(WithContextTest, WithContext) {
  opencensus::context::Context ctx = opencensus::context::Context::Current();
  opencensus::context::WithContext wc(ctx);
}

TEST(WithContextTest, WithContextMovable) {
  opencensus::context::Context ctx = opencensus::context::Context::Current();
  opencensus::context::WithContext wc(std::move(ctx));
}

TEST(WithContextTest, WithContextConditional) {
  opencensus::context::Context ctx = opencensus::context::Context::Current();
  opencensus::context::WithContext wc1(ctx, true);
  opencensus::context::WithContext wc2(ctx, false);
  // TODO: Test swaps.
}

#ifndef NDEBUG
TEST(WithContextDeathTest, DestructorOnWrongThread) {
  opencensus::context::Context ctx = opencensus::context::Context::Current();
  EXPECT_DEBUG_DEATH(
      {
        auto* wc = new opencensus::context::WithContext(ctx);
        std::thread t([wc]() {
          // Running the destructor in a different thread corrupts its
          // thread-local Context. In debug mode, it assert()s.
          delete wc;
        });
        t.join();
      },
      "must be destructed on the same thread");
}
#endif

}  // namespace
