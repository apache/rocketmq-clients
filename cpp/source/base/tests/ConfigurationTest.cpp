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

#include <chrono>
#include <memory>

#include "gtest/gtest.h"
#include "rocketmq/Configuration.h"

ROCKETMQ_NAMESPACE_BEGIN

class ConfigurationTest : public testing::Test {
public:
protected:
  std::string               endpoints_{"8.8.8.8:80;8.8.4.4:80"};
  std::chrono::milliseconds request_timeout_{std::chrono::seconds(1)};
};

TEST_F(ConfigurationTest, testEndpoints) {
  auto configuration = Configuration::newBuilder().withEndpoints(endpoints_).build();
  ASSERT_EQ(endpoints_, configuration.endpoints());
}

TEST_F(ConfigurationTest, testCredentialsProvider) {
  std::string access_key           = "ak";
  std::string access_secret        = "as";
  auto        credentials_provider = std::make_shared<StaticCredentialsProvider>(access_key, access_secret);
  auto        configuration        = Configuration::newBuilder().withCredentialsProvider(credentials_provider).build();
  auto        credentials          = configuration.credentialsProvider()->getCredentials();

  ASSERT_EQ(access_key, credentials.accessKey());
  ASSERT_EQ(access_secret, credentials.accessSecret());
}

TEST_F(ConfigurationTest, testRequestTimeout) {
  auto configuration = Configuration::newBuilder().withRequestTimeout(request_timeout_).build();
  ASSERT_EQ(request_timeout_, configuration.requestTimeout());
}

ROCKETMQ_NAMESPACE_END