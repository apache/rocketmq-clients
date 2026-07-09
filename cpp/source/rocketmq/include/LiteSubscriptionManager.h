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

#include <cstdint>
#include <functional>
#include <memory>
#include <set>
#include <string>
#include <system_error>

#include "ClientConfig.h"
#include "ClientManager.h"
#include "Protocol.h"
#include "Scheduler.h"
#include "Signature.h"
#include "absl/container/flat_hash_set.h"
#include "absl/synchronization/mutex.h"
#include "rocketmq/OffsetOption.h"

ROCKETMQ_NAMESPACE_BEGIN

/**
 * @brief Manages lite topic subscriptions for LitePushConsumer.
 *
 * Responsibilities:
 * - Maintains the set of subscribed lite topics
 * - Synchronizes subscriptions with the server via SyncLiteSubscription RPC
 * - Enforces quota limits (lite_subscription_quota, max_lite_topic_size)
 * - Handles server-pushed NotifyUnsubscribeLiteCommand
 * - Periodically re-syncs all subscriptions (every 30 seconds)
 */
class LiteSubscriptionManager {
public:
  /**
   * @param client_manager Shared pointer to the ClientManager for RPC calls.
   * @param client_config Reference to the ClientConfig for signing and endpoints.
   * @param bind_topic The parent topic name.
   * @param group The consumer group resource.
   */
  LiteSubscriptionManager(std::shared_ptr<ClientManager> client_manager,
                          ClientConfig& client_config,
                          const std::string& bind_topic,
                          const rmq::Resource& group);

  ~LiteSubscriptionManager();

  /**
   * Set the endpoints provider function.
   * Called by LitePushConsumerImpl to provide current route endpoints.
   */
  void setEndpointsProvider(std::function<absl::flat_hash_set<std::string>()> provider) {
    endpoints_provider_ = std::move(provider);
  }

  /**
   * Start the periodic sync task.
   * Performs initial full sync, then schedules every 30 seconds.
   */
  void startUp();

  /**
   * Stop the periodic sync task.
   */
  void shutdown();

  /**
   * Subscribe to a lite topic.
   *
   * @param lite_topic Lite topic name.
   * @param offset_option Optional offset option (nullptr for default).
   * @param ec Error code set on failure.
   */
  void subscribeLite(const std::string& lite_topic,
                     const OffsetOption* offset_option,
                     std::error_code& ec);

  /**
   * Unsubscribe from a lite topic.
   *
   * @param lite_topic Lite topic name.
   * @param ec Error code set on failure.
   */
  void unsubscribeLite(const std::string& lite_topic, std::error_code& ec);

  /**
   * Get a snapshot of currently subscribed lite topics.
   */
  std::set<std::string> getLiteTopicSet() const;

  /**
   * Sync subscription settings from server telemetry.
   * Updates lite_subscription_quota and max_lite_topic_size.
   */
  void syncSettings(const rmq::Settings& settings);

  /**
   * Handle server-pushed unsubscribe command.
   */
  void onNotifyUnsubscribeLiteCommand(const std::string& lite_topic);

  /**
   * Get the bind topic name.
   */
  const std::string& getBindTopicName() const { return bind_topic_; }

  /**
   * Get the consumer group name.
   */
  const std::string& getConsumerGroupName() const { return group_.name(); }

private:
  /**
   * Sync all lite subscriptions to all endpoints (COMPLETE_ADD).
   */
  void syncAllLiteSubscription();

  /**
   * Send SyncLiteSubscription RPC to all route endpoints.
   *
   * @param action Subscription action (PARTIAL_ADD, PARTIAL_REMOVE, COMPLETE_ADD, etc.)
   * @param topics Set of lite topic names to sync.
   * @param offset_option Optional offset option.
   * @param ec Error code set on failure.
   */
  void syncLiteSubscription(rmq::LiteSubscriptionAction action,
                            const std::set<std::string>& topics,
                            const OffsetOption* offset_option,
                            std::error_code& ec);

  /**
   * Validate lite topic name length.
   */
  void validateLiteTopic(const std::string& lite_topic, int max_length, std::error_code& ec);

  /**
   * Check if lite subscription quota allows adding delta more subscriptions.
   */
  void checkLiteSubscriptionQuota(int delta, std::error_code& ec);

  std::shared_ptr<ClientManager> client_manager_;
  ClientConfig& client_config_;
  std::string bind_topic_;
  rmq::Resource group_;

  mutable absl::Mutex lite_topic_set_mtx_;
  absl::flat_hash_set<std::string> lite_topic_set_ GUARDED_BY(lite_topic_set_mtx_);

  std::int32_t lite_subscription_quota_{0};
  std::int32_t max_lite_topic_size_{64};

  std::function<absl::flat_hash_set<std::string>()> endpoints_provider_;

  std::uintptr_t sync_task_handle_{0};
  static const char* SYNC_TASK_NAME;
};

ROCKETMQ_NAMESPACE_END
