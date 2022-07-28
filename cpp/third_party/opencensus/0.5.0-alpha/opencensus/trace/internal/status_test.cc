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

#include "opencensus/trace/exporter/status.h"

#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::HasSubstr;
using ::testing::Not;

namespace {

template <typename StatusT, typename CodeT>
void TestHelper(const CodeT code) {
  StatusT s1;
  StatusT s2(code, "descriptive error here");
  StatusT s3(s2);
  EXPECT_NE("", s1.ToString());
  EXPECT_NE("", s2.ToString());
  EXPECT_NE("", s3.ToString());
  EXPECT_TRUE(s1.ok());
  EXPECT_FALSE(s2.ok());
  EXPECT_FALSE(s3.ok());
  EXPECT_NE(code, s1.CanonicalCode());
  EXPECT_EQ(code, s2.CanonicalCode());
  EXPECT_EQ(code, s3.CanonicalCode());

  EXPECT_THAT(s2.ToString(), HasSubstr("descriptive error here"));

  EXPECT_NE(StatusT(), StatusT(code, "msg2")) << "Equality compares code.";
  EXPECT_NE(StatusT(code, "msg1"), StatusT(code, "msg2"))
      << "Equality compares message.";

  auto ok_code = StatusT().CanonicalCode();
  EXPECT_THAT(StatusT(ok_code, "my message").ToString(),
              Not(HasSubstr("my message")))
      << "OK status ignores message";
}

TEST(OpenCensusStatus, DoesEverything) {
  TestHelper<::opencensus::trace::exporter::Status>(
      ::opencensus::trace::StatusCode::UNAVAILABLE);
}

}  // namespace
