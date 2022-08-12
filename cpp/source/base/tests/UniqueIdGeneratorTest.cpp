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
#include "UniqueIdGenerator.h"
#include "absl/container/flat_hash_set.h"
#include "rocketmq/RocketMQ.h"
#include "spdlog/spdlog.h"
#include "gtest/gtest.h"

#include <iostream>
ROCKETMQ_NAMESPACE_BEGIN

TEST(UniqueIdGeneratorTest, testOutputSampleId) {
  std::cout << "A sample unique ID: " << UniqueIdGenerator::instance().next() << std::endl;
}

TEST(UniqueIdGeneratorTest, testNext) {
  absl::flat_hash_set<std::string> id_set;
  uint32_t total = 500000;
  uint32_t count = 0;
  while (total--) {
    std::string id = UniqueIdGenerator::instance().next();
    if (id_set.contains(id)) {
      SPDLOG_WARN("Yuck, found an duplicated ID: {}", id);
    } else {
      id_set.insert(id);
    }
    ++count;
  }
  EXPECT_EQ(count, id_set.size());
}

ROCKETMQ_NAMESPACE_END