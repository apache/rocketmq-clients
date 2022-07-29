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
#include <cstddef>
#include <unordered_set>

#include "ClientImpl.h"
#include "gtest/gtest.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

TEST(ClientImplTest, testClientId) {
  std::unordered_set<std::string> client_ids;
  for (std::size_t i = 0; i < 128; i++) {
    auto&& client_id = clientId();
    std::cout << client_id << std::endl;
    ASSERT_EQ(client_ids.find(client_id), client_ids.end());
    client_ids.insert(std::move(client_id));
  }
}

ROCKETMQ_NAMESPACE_END