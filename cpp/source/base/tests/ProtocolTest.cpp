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
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#include "Protocol.h"

ROCKETMQ_NAMESPACE_BEGIN

namespace rmq = apache::rocketmq::v2;

class ProtocolTest : public testing::Test {
protected:
  static rmq::MessageQueue makeQueue(const std::string& ns, const std::string& topic, int id,
                                     const std::string& broker, rmq::Permission perm = rmq::Permission::READ_WRITE) {
    rmq::MessageQueue mq;
    mq.mutable_topic()->set_resource_namespace(ns);
    mq.mutable_topic()->set_name(topic);
    mq.set_id(id);
    mq.mutable_broker()->set_name(broker);
    mq.set_permission(perm);
    return mq;
  }

  static rmq::MessageQueue makeQueueWithEndpoints(rmq::AddressScheme scheme,
                                                  const std::vector<std::pair<std::string, int>>& addrs) {
    rmq::MessageQueue mq;
    mq.mutable_topic()->set_resource_namespace("ns");
    mq.mutable_topic()->set_name("topic");
    mq.set_id(0);
    mq.mutable_broker()->set_name("broker-0");
    mq.mutable_broker()->mutable_endpoints()->set_scheme(scheme);
    for (auto& p : addrs) {
      auto* addr = mq.mutable_broker()->mutable_endpoints()->add_addresses();
      addr->set_host(p.first);
      addr->set_port(p.second);
    }
    return mq;
  }
};

TEST_F(ProtocolTest, writablePermissionTest) {
  EXPECT_TRUE(writable(rmq::Permission::WRITE));
  EXPECT_TRUE(writable(rmq::Permission::READ_WRITE));
  EXPECT_FALSE(writable(rmq::Permission::READ));
  EXPECT_FALSE(writable(rmq::Permission::NONE));
}

TEST_F(ProtocolTest, readablePermissionTest) {
  EXPECT_TRUE(readable(rmq::Permission::READ));
  EXPECT_TRUE(readable(rmq::Permission::READ_WRITE));
  EXPECT_FALSE(readable(rmq::Permission::WRITE));
  EXPECT_FALSE(readable(rmq::Permission::NONE));
}

TEST_F(ProtocolTest, messageQueueEqualityTest) {
  auto a = makeQueue("ns", "topic", 0, "broker");
  auto b = makeQueue("ns", "topic", 0, "broker");
  EXPECT_TRUE(a == b);
}

TEST_F(ProtocolTest, messageQueueInequalityTest) {
  auto a = makeQueue("ns", "topic", 0, "broker");
  auto b = makeQueue("ns", "topic", 1, "broker");
  EXPECT_FALSE(a == b);
}

TEST_F(ProtocolTest, urlOfDomainNameTest) {
  auto mq = makeQueueWithEndpoints(rmq::AddressScheme::DOMAIN_NAME, {{"broker.example.com", 8080}});
  EXPECT_EQ("dns:broker.example.com:8080", urlOf(mq));
}

TEST_F(ProtocolTest, urlOfIPv4SingleTest) {
  auto mq = makeQueueWithEndpoints(rmq::AddressScheme::IPv4, {{"10.0.0.1", 8080}});
  EXPECT_EQ("ipv4:10.0.0.1:8080", urlOf(mq));
}

TEST_F(ProtocolTest, urlOfIPv4MultipleTest) {
  auto mq = makeQueueWithEndpoints(rmq::AddressScheme::IPv4, {{"10.0.0.1", 8080}, {"10.0.0.2", 9090}});
  std::string result = urlOf(mq);
  EXPECT_EQ("ipv4:10.0.0.1:8080,10.0.0.2:9090", result);
}

TEST_F(ProtocolTest, urlOfIPv6Test) {
  auto mq = makeQueueWithEndpoints(rmq::AddressScheme::IPv6, {{"fe80::1", 8080}});
  EXPECT_EQ("ipv6:fe80::1:8080", urlOf(mq));
}

TEST_F(ProtocolTest, simpleNameOfFormatTest) {
  auto mq = makeQueue("my-ns", "my-topic", 3, "broker-a");
  EXPECT_EQ("my-ns@my-topic@3@broker-a", simpleNameOf(mq));
}

ROCKETMQ_NAMESPACE_END
