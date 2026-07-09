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
#include <cstddef>
#include <functional>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>

#include "ClientImpl.h"
#include "ClientManagerMock.h"
#include "NameServerResolverMock.h"
#include "ProducerImpl.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "rocketmq/ErrorCode.h"
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

// Test-only subclass that exposes the protected accessPoint() method.
class TestableProducerImpl : public ProducerImpl {
public:
  TestableProducerImpl() : ClientImpl("") {}

  rmq::Endpoints testAccessPoint() {
    return accessPoint();
  }
};

class ClientImplLifecycleTest : public ::testing::Test {
protected:
  void SetUp() override {
    resolver_mock_ = std::make_shared<testing::NiceMock<NameServerResolverMock>>();
    client_manager_ = std::make_shared<testing::NiceMock<ClientManagerMock>>();
  }

  void TearDown() override {
    producer_.reset();
    testable_producer_.reset();
    client_manager_.reset();
    resolver_mock_.reset();
  }

  std::shared_ptr<ProducerImpl> producer_;
  std::shared_ptr<TestableProducerImpl> testable_producer_;
  std::shared_ptr<testing::NiceMock<NameServerResolverMock>> resolver_mock_;
  std::shared_ptr<testing::NiceMock<ClientManagerMock>> client_manager_;
};

TEST_F(ClientImplLifecycleTest, startWithoutResolverThrowsTest) {
  producer_ = std::make_shared<ProducerImpl>();
  // No resolver configured — start() must throw.
  EXPECT_THROW(producer_->start(), std::runtime_error);
}

TEST_F(ClientImplLifecycleTest, shutdownWithoutStartIsNoopTest) {
  producer_ = std::make_shared<ProducerImpl>();
  // State defaults to CREATED. shutdown() on a non-started client must be a safe no-op.
  EXPECT_NO_THROW(producer_->shutdown());
  EXPECT_FALSE(producer_->active());
}

TEST_F(ClientImplLifecycleTest, doubleShutdownIsSafeTest) {
  producer_ = std::make_shared<ProducerImpl>();
  producer_->state(State::STARTED);

  // First shutdown transitions from STARTED → STOPPED.
  EXPECT_NO_THROW(producer_->shutdown());
  EXPECT_FALSE(producer_->active());

  // Second shutdown must be a safe no-op (CAS fails, returns immediately).
  EXPECT_NO_THROW(producer_->shutdown());
  EXPECT_FALSE(producer_->active());
}

TEST_F(ClientImplLifecycleTest, accessPointParsesIPv4Test) {
  testable_producer_ = std::make_shared<TestableProducerImpl>();
  ON_CALL(*resolver_mock_, resolve()).WillByDefault(testing::Return(std::string("ipv4:10.0.0.1:8080")));
  testable_producer_->withNameServerResolver(resolver_mock_);

  rmq::Endpoints ep = testable_producer_->testAccessPoint();
  EXPECT_EQ(ep.scheme(), rmq::AddressScheme::IPv4);
  ASSERT_EQ(ep.addresses_size(), 1);
  EXPECT_EQ(ep.addresses(0).host(), "10.0.0.1");
  EXPECT_EQ(ep.addresses(0).port(), 8080);
}

TEST_F(ClientImplLifecycleTest, accessPointParsesDNSTest) {
  testable_producer_ = std::make_shared<TestableProducerImpl>();
  ON_CALL(*resolver_mock_, resolve()).WillByDefault(testing::Return(std::string("dns:broker.example.com:9876")));
  testable_producer_->withNameServerResolver(resolver_mock_);

  rmq::Endpoints ep = testable_producer_->testAccessPoint();
  EXPECT_EQ(ep.scheme(), rmq::AddressScheme::DOMAIN_NAME);
  ASSERT_EQ(ep.addresses_size(), 1);
  EXPECT_EQ(ep.addresses(0).host(), "broker.example.com");
  EXPECT_EQ(ep.addresses(0).port(), 9876);
}

TEST_F(ClientImplLifecycleTest, accessPointParsesMultipleIPv4Test) {
  testable_producer_ = std::make_shared<TestableProducerImpl>();
  ON_CALL(*resolver_mock_, resolve())
      .WillByDefault(testing::Return(std::string("ipv4:10.0.0.1:8080,10.0.0.2:9090")));
  testable_producer_->withNameServerResolver(resolver_mock_);

  rmq::Endpoints ep = testable_producer_->testAccessPoint();
  EXPECT_EQ(ep.scheme(), rmq::AddressScheme::IPv4);
  ASSERT_EQ(ep.addresses_size(), 2);
  EXPECT_EQ(ep.addresses(0).host(), "10.0.0.1");
  EXPECT_EQ(ep.addresses(0).port(), 8080);
  EXPECT_EQ(ep.addresses(1).host(), "10.0.0.2");
  EXPECT_EQ(ep.addresses(1).port(), 9090);
}

TEST_F(ClientImplLifecycleTest, scheduleWithNullManagerDoesNotCrashTest) {
  producer_ = std::make_shared<ProducerImpl>();
  // No client_manager set — schedule() must log a warning and return safely.
  EXPECT_NO_THROW(
      producer_->schedule("test-task", []() {}, std::chrono::milliseconds(100)));
}

TEST_F(ClientImplLifecycleTest, activeReflectsStateTest) {
  producer_ = std::make_shared<ProducerImpl>();

  // Default state is CREATED → not active.
  EXPECT_FALSE(producer_->active());

  // Transition to STARTED → active.
  producer_->state(State::STARTED);
  EXPECT_TRUE(producer_->active());

  // Transition to STOPPED → not active.
  producer_->state(State::STOPPED);
  EXPECT_FALSE(producer_->active());
}

ROCKETMQ_NAMESPACE_END