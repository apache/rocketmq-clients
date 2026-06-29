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
#include <memory>
#include <set>
#include <string>
#include <system_error>

#include "ClientManagerMock.h"
#include "LiteSubscriptionManager.h"
#include "Protocol.h"
#include "rocketmq/ErrorCode.h"
#include "rocketmq/OffsetOption.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::NiceMock;

ROCKETMQ_NAMESPACE_BEGIN

class LiteSubscriptionManagerTest : public ::testing::Test {
protected:
  void SetUp() override {
    mock_client_manager_ = std::make_shared<NiceMock<ClientManagerMock>>();

    // Setup default mock behavior for syncLiteSubscription - return success
    ON_CALL(*mock_client_manager_, syncLiteSubscription(_, _, _, _, _))
        .WillByDefault([](const std::string&, const Metadata&, const SyncLiteSubscriptionRequest&,
                          std::chrono::milliseconds,
                          const std::function<void(const std::error_code&, const SyncLiteSubscriptionResponse&)>& cb) {
          SyncLiteSubscriptionResponse response;
          cb(std::error_code{}, response);
        });

    // Set up client config
    client_config_.client_id = "test-client-id";
    client_config_.request_timeout = absl::FromChrono(std::chrono::seconds(3));

    // Set up group resource
    group_.set_resource_namespace("test-namespace");
    group_.set_name("test-consumer-group");
  }

  std::shared_ptr<NiceMock<ClientManagerMock>> mock_client_manager_;
  ClientConfig client_config_;
  rmq::Resource group_;
  std::string bind_topic_ = "test-bind-topic";
};

// ============================================================================
// Constructor Tests
// ============================================================================

TEST_F(LiteSubscriptionManagerTest, testConstructor) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  EXPECT_EQ(manager.getBindTopicName(), "test-bind-topic");
  EXPECT_EQ(manager.getConsumerGroupName(), "test-consumer-group");
}

TEST_F(LiteSubscriptionManagerTest, testInitialLiteTopicSetIsEmpty) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  auto topic_set = manager.getLiteTopicSet();
  EXPECT_TRUE(topic_set.empty());
}

// ============================================================================
// syncSettings Tests
// ============================================================================

TEST_F(LiteSubscriptionManagerTest, testSyncSettingsWithSubscription) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  rmq::Settings settings;
  auto* subscription = settings.mutable_subscription();
  subscription->set_lite_subscription_quota(10);
  subscription->set_max_lite_topic_size(128);

  manager.syncSettings(settings);

  // Settings should be updated - we can verify by trying to subscribe
  // (quota check will pass with quota=10)
  std::error_code ec;
  absl::flat_hash_set<std::string> endpoints = {"localhost:8081"};
  manager.setEndpointsProvider([&endpoints]() { return endpoints; });
  manager.subscribeLite("test-topic", nullptr, ec);
  EXPECT_FALSE(ec);
}

TEST_F(LiteSubscriptionManagerTest, testSyncSettingsWithoutSubscription) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  rmq::Settings settings;
  // No subscription field

  manager.syncSettings(settings);

  // Default max_lite_topic_size should remain 64
  // Try to subscribe with a topic longer than 64 characters
  std::error_code ec;
  absl::flat_hash_set<std::string> endpoints = {"localhost:8081"};
  manager.setEndpointsProvider([&endpoints]() { return endpoints; });
  std::string long_topic(65, 'a');
  manager.subscribeLite(long_topic, nullptr, ec);
  EXPECT_EQ(ec, ErrorCode::IllegalLiteTopic);
}

// ============================================================================
// subscribeLite Tests
// ============================================================================

TEST_F(LiteSubscriptionManagerTest, testSubscribeLiteSuccess) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  // Set quota
  rmq::Settings settings;
  settings.mutable_subscription()->set_lite_subscription_quota(10);
  manager.syncSettings(settings);

  absl::flat_hash_set<std::string> endpoints = {"localhost:8081"};
  manager.setEndpointsProvider([&endpoints]() { return endpoints; });

  std::error_code ec;
  manager.subscribeLite("test-topic", nullptr, ec);

  EXPECT_FALSE(ec);
  auto topic_set = manager.getLiteTopicSet();
  EXPECT_EQ(topic_set.size(), 1);
  EXPECT_TRUE(topic_set.count("test-topic"));
}

TEST_F(LiteSubscriptionManagerTest, testSubscribeLiteAlreadySubscribed) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  rmq::Settings settings;
  settings.mutable_subscription()->set_lite_subscription_quota(10);
  manager.syncSettings(settings);

  absl::flat_hash_set<std::string> endpoints = {"localhost:8081"};
  manager.setEndpointsProvider([&endpoints]() { return endpoints; });

  std::error_code ec;
  manager.subscribeLite("test-topic", nullptr, ec);
  EXPECT_FALSE(ec);

  // Subscribe again - should be idempotent
  manager.subscribeLite("test-topic", nullptr, ec);
  EXPECT_FALSE(ec);

  auto topic_set = manager.getLiteTopicSet();
  EXPECT_EQ(topic_set.size(), 1);
}

TEST_F(LiteSubscriptionManagerTest, testSubscribeLiteWithOffsetOption) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  rmq::Settings settings;
  settings.mutable_subscription()->set_lite_subscription_quota(10);
  manager.syncSettings(settings);

  absl::flat_hash_set<std::string> endpoints = {"localhost:8081"};
  manager.setEndpointsProvider([&endpoints]() { return endpoints; });

  std::error_code ec;
  auto offset = OffsetOption::ofOffset(100);
  manager.subscribeLite("test-topic", &offset, ec);

  EXPECT_FALSE(ec);
  auto topic_set = manager.getLiteTopicSet();
  EXPECT_TRUE(topic_set.count("test-topic"));
}

TEST_F(LiteSubscriptionManagerTest, testSubscribeLiteBlankTopic) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  std::error_code ec;
  manager.subscribeLite("", nullptr, ec);
  EXPECT_EQ(ec, ErrorCode::IllegalLiteTopic);
}

TEST_F(LiteSubscriptionManagerTest, testSubscribeLiteTopicTooLong) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  // Default max_lite_topic_size is 64
  std::string long_topic(65, 'a');

  std::error_code ec;
  manager.subscribeLite(long_topic, nullptr, ec);
  EXPECT_EQ(ec, ErrorCode::IllegalLiteTopic);
}

TEST_F(LiteSubscriptionManagerTest, testSubscribeLiteQuotaExceeded) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  // Set quota to 0
  rmq::Settings settings;
  settings.mutable_subscription()->set_lite_subscription_quota(0);
  manager.syncSettings(settings);

  std::error_code ec;
  manager.subscribeLite("test-topic", nullptr, ec);
  EXPECT_EQ(ec, ErrorCode::LiteSubscriptionQuotaExceeded);
}

TEST_F(LiteSubscriptionManagerTest, testSubscribeLiteNoEndpoints) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  rmq::Settings settings;
  settings.mutable_subscription()->set_lite_subscription_quota(10);
  manager.syncSettings(settings);

  // No endpoints provider set - endpoints will be empty
  std::error_code ec;
  manager.subscribeLite("test-topic", nullptr, ec);
  EXPECT_EQ(ec, ErrorCode::NotFound);
}

// ============================================================================
// unsubscribeLite Tests
// ============================================================================

TEST_F(LiteSubscriptionManagerTest, testUnsubscribeLiteSuccess) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  rmq::Settings settings;
  settings.mutable_subscription()->set_lite_subscription_quota(10);
  manager.syncSettings(settings);

  absl::flat_hash_set<std::string> endpoints = {"localhost:8081"};
  manager.setEndpointsProvider([&endpoints]() { return endpoints; });

  std::error_code ec;
  manager.subscribeLite("test-topic", nullptr, ec);
  EXPECT_FALSE(ec);

  manager.unsubscribeLite("test-topic", ec);
  EXPECT_FALSE(ec);

  auto topic_set = manager.getLiteTopicSet();
  EXPECT_TRUE(topic_set.empty());
}

TEST_F(LiteSubscriptionManagerTest, testUnsubscribeLiteNotSubscribed) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  std::error_code ec;
  manager.unsubscribeLite("non-existent-topic", ec);
  // Should not set error, just return silently
  EXPECT_FALSE(ec);

  auto topic_set = manager.getLiteTopicSet();
  EXPECT_TRUE(topic_set.empty());
}

// ============================================================================
// onNotifyUnsubscribeLiteCommand Tests
// ============================================================================

TEST_F(LiteSubscriptionManagerTest, testOnNotifyUnsubscribeLiteCommand) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  rmq::Settings settings;
  settings.mutable_subscription()->set_lite_subscription_quota(10);
  manager.syncSettings(settings);

  absl::flat_hash_set<std::string> endpoints = {"localhost:8081"};
  manager.setEndpointsProvider([&endpoints]() { return endpoints; });

  std::error_code ec;
  manager.subscribeLite("test-topic", nullptr, ec);
  EXPECT_FALSE(ec);

  // Server pushes unsubscribe command
  manager.onNotifyUnsubscribeLiteCommand("test-topic");

  auto topic_set = manager.getLiteTopicSet();
  EXPECT_FALSE(topic_set.count("test-topic"));
}

TEST_F(LiteSubscriptionManagerTest, testOnNotifyUnsubscribeLiteCommandWithEmptyTopic) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  rmq::Settings settings;
  settings.mutable_subscription()->set_lite_subscription_quota(10);
  manager.syncSettings(settings);

  absl::flat_hash_set<std::string> endpoints = {"localhost:8081"};
  manager.setEndpointsProvider([&endpoints]() { return endpoints; });

  std::error_code ec;
  manager.subscribeLite("test-topic", nullptr, ec);
  EXPECT_FALSE(ec);

  // Empty topic should not remove anything
  manager.onNotifyUnsubscribeLiteCommand("");

  auto topic_set = manager.getLiteTopicSet();
  EXPECT_TRUE(topic_set.count("test-topic"));
}

// ============================================================================
// Multiple Operations Tests
// ============================================================================

TEST_F(LiteSubscriptionManagerTest, testMultipleOperationsSequence) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  rmq::Settings settings;
  settings.mutable_subscription()->set_lite_subscription_quota(10);
  manager.syncSettings(settings);

  absl::flat_hash_set<std::string> endpoints = {"localhost:8081"};
  manager.setEndpointsProvider([&endpoints]() { return endpoints; });

  std::error_code ec;

  // Subscribe to multiple topics
  manager.subscribeLite("topic1", nullptr, ec);
  EXPECT_FALSE(ec);

  auto offset = OffsetOption::minOffset();
  manager.subscribeLite("topic2", &offset, ec);
  EXPECT_FALSE(ec);

  // Subscribe to topic1 again (should be idempotent)
  manager.subscribeLite("topic1", nullptr, ec);
  EXPECT_FALSE(ec);

  auto topic_set = manager.getLiteTopicSet();
  EXPECT_EQ(topic_set.size(), 2);
  EXPECT_TRUE(topic_set.count("topic1"));
  EXPECT_TRUE(topic_set.count("topic2"));

  // Unsubscribe topic1
  manager.unsubscribeLite("topic1", ec);
  EXPECT_FALSE(ec);

  topic_set = manager.getLiteTopicSet();
  EXPECT_EQ(topic_set.size(), 1);
  EXPECT_FALSE(topic_set.count("topic1"));
  EXPECT_TRUE(topic_set.count("topic2"));
}

TEST_F(LiteSubscriptionManagerTest, testGetLiteTopicSetReturnsCopy) {
  LiteSubscriptionManager manager(mock_client_manager_, client_config_, bind_topic_, group_);

  rmq::Settings settings;
  settings.mutable_subscription()->set_lite_subscription_quota(10);
  manager.syncSettings(settings);

  absl::flat_hash_set<std::string> endpoints = {"localhost:8081"};
  manager.setEndpointsProvider([&endpoints]() { return endpoints; });

  std::error_code ec;
  manager.subscribeLite("topic1", nullptr, ec);
  EXPECT_FALSE(ec);

  // Get set and modify it
  auto topic_set = manager.getLiteTopicSet();
  topic_set.insert("should-not-appear");

  // Original should not be affected
  auto actual_set = manager.getLiteTopicSet();
  EXPECT_EQ(actual_set.size(), 1);
  EXPECT_FALSE(actual_set.count("should-not-appear"));
}

ROCKETMQ_NAMESPACE_END
