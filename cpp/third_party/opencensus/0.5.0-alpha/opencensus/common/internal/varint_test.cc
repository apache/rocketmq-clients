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

#include "opencensus/common/internal/varint.h"

#include <iostream>

#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"
#include "gtest/gtest.h"

namespace opencensus {
namespace common {
namespace {

TEST(Varint, EncodeDecode) {
  auto test = [](uint32_t i) {
    std::string s;
    AppendVarint32(i, &s);
    std::cout << " int " << i << " encoded to hex " << absl::BytesToHexString(s)
              << "\n";
    absl::string_view sv(s);
    uint32_t j = i + 1;
    EXPECT_TRUE(ParseVarint32(&sv, &j));
    EXPECT_EQ(i, j);
  };
  test(0);
  test(1);
  test(10);
  test(100);

  test(127);
  test(128);
  test(129);

  test(255);
  test(256);
  test(257);

  test(16383);
  test(16384);
  test(16385);

  test(2097151);
  test(2097152);
  test(2097153);

  test(268435455);
  test(268435456);
  test(268435457);

  test(4294967295);
}

TEST(Varint, DecodeOutOfRange) {
  constexpr uint8_t input[] = {0xff, 0xff, 0xff, 0xff, 0x10};
  absl::string_view sv(reinterpret_cast<const char*>(input), sizeof(input));
  uint32_t i;
  EXPECT_FALSE(ParseVarint32(&sv, &i));
}

}  // namespace
}  // namespace common
}  // namespace opencensus
