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
#include <system_error>

#include "MixAll.h"
#include "absl/strings/str_split.h"
#include "gtest/gtest.h"

ROCKETMQ_NAMESPACE_BEGIN

TEST(MixAllTest, testOsName) {
  const char* os_name = MixAll::osName();
  std::cout << os_name << std::endl;
}

TEST(MixAllTest, testValidate) {
  auto message = Message::newBuilder().withTopic("").build();
  std::error_code ec;
  MixAll::validate(*message, ec);
  ASSERT_EQ(true, (bool)ec);
  ASSERT_TRUE(absl::StrContains(ec.message(), "illegal"));
}

ROCKETMQ_NAMESPACE_END