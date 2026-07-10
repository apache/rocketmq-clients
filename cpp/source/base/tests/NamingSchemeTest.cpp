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
#include <string>
#include <vector>

#include "NamingScheme.h"
#include "absl/strings/match.h"
#include "gtest/gtest.h"

ROCKETMQ_NAMESPACE_BEGIN

class NamingSchemeTest : public testing::Test {
protected:
  NamingScheme naming_scheme;
};

TEST_F(NamingSchemeTest, acceptDnsPrefix) {
  EXPECT_TRUE(naming_scheme.accept("dns:example.com:8080"));
}

TEST_F(NamingSchemeTest, acceptIPv4Prefix) {
  EXPECT_TRUE(naming_scheme.accept("ipv4:10.0.0.1:8080"));
}

TEST_F(NamingSchemeTest, acceptIPv6Prefix) {
  EXPECT_TRUE(naming_scheme.accept("ipv6:fe80::1:8080"));
}

TEST_F(NamingSchemeTest, rejectUnknownPrefix) {
  EXPECT_FALSE(naming_scheme.accept("http://example.com"));
  EXPECT_FALSE(naming_scheme.accept("10.0.0.1:8080"));
  EXPECT_FALSE(naming_scheme.accept(""));
}

TEST_F(NamingSchemeTest, buildAddressFromIPv4List) {
  std::vector<std::string> list = {"10.0.0.1:8080", "10.0.0.2:9090"};
  std::string result = naming_scheme.buildAddress(list);
  EXPECT_TRUE(absl::StartsWith(result, "ipv4:"));
  // Both addresses should appear
  EXPECT_NE(std::string::npos, result.find("10.0.0.1:8080"));
  EXPECT_NE(std::string::npos, result.find("10.0.0.2:9090"));
}

TEST_F(NamingSchemeTest, buildAddressFromDnsShortCircuits) {
  // DNS record found → return immediately, ignoring IPv4 entries
  std::vector<std::string> list = {"10.0.0.1:8080", "broker.example.com:9876"};
  std::string result = naming_scheme.buildAddress(list);
  EXPECT_EQ("dns:broker.example.com:9876", result);
}

TEST_F(NamingSchemeTest, buildAddressSkipsMalformedEntries) {
  std::vector<std::string> list = {"no-port", "10.0.0.1:8080"};
  std::string result = naming_scheme.buildAddress(list);
  EXPECT_TRUE(absl::StartsWith(result, "ipv4:"));
}

TEST_F(NamingSchemeTest, buildAddressEmptyListReturnsEmpty) {
  std::vector<std::string> list;
  std::string result = naming_scheme.buildAddress(list);
  EXPECT_TRUE(result.empty());
}

ROCKETMQ_NAMESPACE_END
