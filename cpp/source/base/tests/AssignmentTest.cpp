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
#include <algorithm>

#include "Protocol.h"

ROCKETMQ_NAMESPACE_BEGIN

class AssignmentTest : public testing::Test {
public:
  void SetUp() override {
  }

  void TearDown() override {
  }
};

TEST_F(AssignmentTest, testSort) {
  std::vector<rmq::Assignment> assignments;
  std::sort(assignments.begin(), assignments.end(),
            [](const rmq::Assignment& lhs, const rmq::Assignment& rhs) { return lhs < rhs; });
}

ROCKETMQ_NAMESPACE_END