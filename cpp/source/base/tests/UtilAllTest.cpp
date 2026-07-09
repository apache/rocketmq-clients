/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "gtest/gtest.h"
#include "UtilAll.h"

#include <string>

ROCKETMQ_NAMESPACE_BEGIN

class UtilAllTest : public testing::Test {};

TEST_F(UtilAllTest, compressAndUncompressRoundTripTest) {
  std::string original = "Hello, RocketMQ! This is a test of compression and decompression.";
  std::string compressed;
  ASSERT_TRUE(UtilAll::compress(original, compressed));
  EXPECT_FALSE(compressed.empty());
  EXPECT_NE(original, compressed);

  std::string decompressed;
  ASSERT_TRUE(UtilAll::uncompress(compressed, decompressed));
  EXPECT_EQ(original, decompressed);
}

TEST_F(UtilAllTest, compressEmptyStringTest) {
  std::string original;
  std::string compressed;
  ASSERT_TRUE(UtilAll::compress(original, compressed));

  std::string decompressed;
  ASSERT_TRUE(UtilAll::uncompress(compressed, decompressed));
  EXPECT_EQ(original, decompressed);
}

TEST_F(UtilAllTest, compressLargePayloadTest) {
  // 64KB of repeated data — should compress well
  std::string original(65536, 'A');
  std::string compressed;
  ASSERT_TRUE(UtilAll::compress(original, compressed));
  EXPECT_LT(compressed.size(), original.size());

  std::string decompressed;
  ASSERT_TRUE(UtilAll::uncompress(compressed, decompressed));
  EXPECT_EQ(original, decompressed);
}

TEST_F(UtilAllTest, uncompressInvalidDataFailsTest) {
  std::string garbage = "this is not zlib compressed data";
  std::string output;
  EXPECT_FALSE(UtilAll::uncompress(garbage, output));
}

ROCKETMQ_NAMESPACE_END
