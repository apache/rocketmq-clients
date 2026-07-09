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
#pragma once

#include <memory>
#include <set>
#include <string>
#include <system_error>

#include "LiteSubscriptionManager.h"
#include "PushConsumerImpl.h"
#include "absl/strings/string_view.h"
#include "rocketmq/OffsetOption.h"

ROCKETMQ_NAMESPACE_BEGIN

/**
 * @brief Internal implementation of LitePushConsumer.
 *
 * Extends PushConsumerImpl with lite subscription management:
 * - Uses ClientType::LITE_PUSH_CONSUMER
 * - Creates and manages LiteSubscriptionManager for dynamic lite topic subscriptions
 * - Handles NotifyUnsubscribeLiteCommand from server telemetry
 * - Syncs lite subscription settings from server
 */
class LitePushConsumerImpl : public PushConsumerImpl {
public:
  explicit LitePushConsumerImpl(absl::string_view group_name, const std::string& bind_topic);

  ~LitePushConsumerImpl() override;

  void start() override;

  void shutdown() override;

  void buildClientSettings(rmq::Settings& settings) override
      LOCKS_EXCLUDED(topic_filter_expression_table_mtx_);

  void prepareHeartbeatData(HeartbeatRequest& request) override;

  /**
   * Subscribe to a lite topic with optional offset.
   */
  void subscribeLite(const std::string& lite_topic, const OffsetOption* offset_option, std::error_code& ec);

  /**
   * Unsubscribe from a lite topic.
   */
  void unsubscribeLite(const std::string& lite_topic, std::error_code& ec);

  /**
   * Get current lite topic set.
   */
  std::set<std::string> getLiteTopicSet() const;

  /**
   * Get the bind topic name.
   */
  const std::string& getBindTopicName() const { return bind_topic_; }

  /**
   * Handle NotifyUnsubscribeLiteCommand from server.
   */
  void onNotifyUnsubscribeLiteCommand(const std::string& lite_topic) override;

  /**
   * Handle settings update from server telemetry.
   * Called when server pushes new settings via telemetry stream.
   */
  void onSettingsUpdate(const rmq::Settings& settings);

  LiteSubscriptionManager& liteSubscriptionManager() { return *lite_subscription_manager_; }

protected:
  std::shared_ptr<ClientImpl> self() override {
    return shared_from_this();
  }

private:
  std::string bind_topic_;
  std::unique_ptr<LiteSubscriptionManager> lite_subscription_manager_;

  /**
   * Provide current route endpoints to LiteSubscriptionManager.
   */
  absl::flat_hash_set<std::string> getCurrentEndpoints();
};

ROCKETMQ_NAMESPACE_END
