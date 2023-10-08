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

#include <tuple>
#include <vector>

#include "NamingScheme.h"
#include "StaticNameServerResolver.h"
#include "absl/strings/str_join.h"
#include "absl/strings/str_split.h"
#include "gtest/gtest.h"
#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

TEST(StaticNameServerResolverTest, testCtor) {
  std::string addr1 = "127.0.0.1:9876";
  std::string addr2 = "10.0.0.1:9876";
  std::string endpoint = absl::StrJoin(std::vector<std::string>{addr1, addr2}, ";");

  StaticNameServerResolver resolver(endpoint);
  std::string result = resolver.resolve();

  ASSERT_TRUE(absl::StartsWith(result, NamingScheme::IPv4Prefix));
  ASSERT_TRUE(absl::StrContains(result, addr1));
  ASSERT_TRUE(absl::StrContains(result, addr2));

  std::string endpoint2 = absl::StrJoin(std::vector<std::string>{addr1, addr2}, ",");
  StaticNameServerResolver resolver2(endpoint2);
  std::string result2 = resolver2.resolve();
  ASSERT_TRUE(absl::StartsWith(result2, NamingScheme::IPv4Prefix));
  ASSERT_TRUE(absl::StrContains(result2, addr1));
  ASSERT_TRUE(absl::StrContains(result2, addr2));

  std::string grpc_naming_endpoint = "ipv4:127.0.0.1:9876,10.0.0.1:9876";
  StaticNameServerResolver resolver3(grpc_naming_endpoint);
  ASSERT_EQ(grpc_naming_endpoint, resolver3.resolve());

  std::string grpc_naming_endpoint2 = "dns:localhost:9876";
  StaticNameServerResolver resolver4(grpc_naming_endpoint2);
  ASSERT_EQ(grpc_naming_endpoint2, resolver4.resolve());
}

ROCKETMQ_NAMESPACE_END