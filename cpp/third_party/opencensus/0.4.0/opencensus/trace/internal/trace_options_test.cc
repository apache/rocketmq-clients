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

#include "opencensus/trace/trace_options.h"

#include "gtest/gtest.h"

namespace opencensus {
namespace trace {

namespace {

TEST(TraceOptionsTest, SetSampled) {
  TraceOptions opts;

  EXPECT_EQ("00", opts.ToHex());
  EXPECT_FALSE(opts.IsSampled());
  opts = opts.WithSampling(true);
  EXPECT_EQ("01", opts.ToHex());
  EXPECT_TRUE(opts.IsSampled());
  opts = opts.WithSampling(false);
  EXPECT_EQ("00", opts.ToHex());
  EXPECT_FALSE(opts.IsSampled());
}

TEST(TraceOptionsTest, Comparison) {
  TraceOptions a;
  TraceOptions b;
  EXPECT_EQ(a, b);
  b = b.WithSampling(true);
  EXPECT_FALSE(a == b);
}

TEST(TraceOptionsTest, CopyAndSetSampled) {
  TraceOptions a = TraceOptions().WithSampling(true);
  TraceOptions b = a.WithSampling(false);
  EXPECT_TRUE(a.IsSampled());
  EXPECT_FALSE(b.IsSampled());
}

}  // namespace
}  // namespace trace
}  // namespace opencensus
