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

#include "opencensus/common/internal/hostname.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

#include <string>

namespace opencensus {
namespace common {
namespace {

TEST(HostnameTest, OpenCensusTaskSmokeTest) {
  const std::string task = OpenCensusTask();
  std::cout << "opencensus_task = \"" << task << "\"\n";
  EXPECT_THAT(task, ::testing::StartsWith("cpp-"));
}

}  // namespace
}  // namespace common
}  // namespace opencensus
