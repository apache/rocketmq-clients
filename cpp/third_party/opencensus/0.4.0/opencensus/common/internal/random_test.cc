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

#include "opencensus/common/internal/random.h"
#include "gtest/gtest.h"

namespace opencensus {
namespace common {

TEST(RandomTest, GenerateValues) {
  Random* rand = Random::GetRandom();
  for (int i = 0; i < 1000; ++i) {
    float value = rand->GenerateRandomFloat();
    EXPECT_LE(value, 1.0);
    EXPECT_GE(value, 0.0);
  }

  for (int i = 0; i < 1000; ++i) {
    double value = rand->GenerateRandomDouble();
    EXPECT_LE(value, 1.0);
    EXPECT_GE(value, 0.0);
  }
}

}  // namespace common
}  // namespace opencensus
