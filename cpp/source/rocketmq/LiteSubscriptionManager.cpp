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
#include "LiteSubscriptionManager.h"

#include <algorithm>
#include <chrono>
#include <memory>
#include <set>
#include <string>

#include "MixAll.h"
#include "Protocol.h"
#include "rocketmq/ErrorCode.h"
#include "rocketmq/OffsetOption.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

const char* LiteSubscriptionManager::SYNC_TASK_NAME = "lite-subscription-sync-task";

LiteSubscriptionManager::LiteSubscriptionManager(std::shared_ptr<ClientManager> client_manager,
                                                 ClientConfig& client_config,
                                                 const std::string& bind_topic,
                                                 const rmq::Resource& group)
    : client_manager_(std::move(client_manager)),
      client_config_(client_config),
      bind_topic_(bind_topic),
      group_(group) {
  // Sync quota from client config (may have been set from server telemetry)
  lite_subscription_quota_ = client_config_.subscriber.lite_subscription_quota;
  max_lite_topic_size_ = client_config_.subscriber.max_lite_topic_size;
}

LiteSubscriptionManager::~LiteSubscriptionManager() {
  shutdown();
}

void LiteSubscriptionManager::startUp() {
  // Perform initial full sync
  syncAllLiteSubscription();

  // Schedule periodic full sync every 30 seconds
  auto weak_self = std::weak_ptr<LiteSubscriptionManager>();
  // Note: We cannot use shared_from_this here since LiteSubscriptionManager
  // doesn't inherit from enable_shared_from_this. Use raw capture instead.
  // The scheduler will be cancelled on shutdown.
  auto sync_functor = [this]() {
    syncAllLiteSubscription();
  };

  sync_task_handle_ = client_manager_->getScheduler()->schedule(
      sync_functor, SYNC_TASK_NAME,
      std::chrono::seconds(30), std::chrono::seconds(30));
  SPDLOG_INFO("LiteSubscriptionManager started for bindTopic={}, group={}",
              bind_topic_, group_.name());
}

void LiteSubscriptionManager::shutdown() {
  if (sync_task_handle_) {
    client_manager_->getScheduler()->cancel(sync_task_handle_);
    sync_task_handle_ = 0;
    SPDLOG_INFO("LiteSubscriptionManager stopped for bindTopic={}, group={}",
                bind_topic_, group_.name());
  }
}

void LiteSubscriptionManager::subscribeLite(const std::string& lite_topic,
                                            const OffsetOption* offset_option,
                                            std::error_code& ec) {
  {
    absl::MutexLock lk(&lite_topic_set_mtx_);
    if (lite_topic_set_.contains(lite_topic)) {
      SPDLOG_DEBUG("Lite topic already subscribed: {}, bindTopic={}, group={}",
                   lite_topic, bind_topic_, group_.name());
      return;
    }
  }

  validateLiteTopic(lite_topic, max_lite_topic_size_, ec);
  if (ec) {
    return;
  }

  checkLiteSubscriptionQuota(1, ec);
  if (ec) {
    SPDLOG_ERROR("Failed to subscribeLite {}: quota exceeded, bindTopic={}, group={}",
                 lite_topic, bind_topic_, group_.name());
    return;
  }

  std::set<std::string> topics = {lite_topic};
  syncLiteSubscription(rmq::LiteSubscriptionAction::PARTIAL_ADD, topics, offset_option, ec);
  if (ec) {
    SPDLOG_ERROR("Failed to subscribeLite {}, bindTopic={}, group={}",
                 lite_topic, bind_topic_, group_.name());
    return;
  }

  {
    absl::MutexLock lk(&lite_topic_set_mtx_);
    lite_topic_set_.insert(lite_topic);
  }
  SPDLOG_INFO("SubscribeLite {}, bindTopic={}, group={}, clientId={}",
              lite_topic, bind_topic_, group_.name(), client_config_.client_id);
}

void LiteSubscriptionManager::unsubscribeLite(const std::string& lite_topic, std::error_code& ec) {
  {
    absl::MutexLock lk(&lite_topic_set_mtx_);
    if (!lite_topic_set_.contains(lite_topic)) {
      SPDLOG_DEBUG("Lite topic not subscribed: {}, bindTopic={}, group={}",
                   lite_topic, bind_topic_, group_.name());
      return;
    }
  }

  std::set<std::string> topics = {lite_topic};
  syncLiteSubscription(rmq::LiteSubscriptionAction::PARTIAL_REMOVE, topics, nullptr, ec);
  if (ec) {
    SPDLOG_ERROR("Failed to unsubscribeLite {}, bindTopic={}, group={}",
                 lite_topic, bind_topic_, group_.name());
    return;
  }

  {
    absl::MutexLock lk(&lite_topic_set_mtx_);
    lite_topic_set_.erase(lite_topic);
  }
  SPDLOG_INFO("UnsubscribeLite {}, bindTopic={}, group={}, clientId={}",
              lite_topic, bind_topic_, group_.name(), client_config_.client_id);
}

std::set<std::string> LiteSubscriptionManager::getLiteTopicSet() const {
  absl::MutexLock lk(&lite_topic_set_mtx_);
  return std::set<std::string>(lite_topic_set_.begin(), lite_topic_set_.end());
}

void LiteSubscriptionManager::syncSettings(const rmq::Settings& settings) {
  if (!settings.has_subscription()) {
    return;
  }
  const auto& subscription = settings.subscription();
  if (subscription.has_lite_subscription_quota()) {
    lite_subscription_quota_ = subscription.lite_subscription_quota();
    SPDLOG_INFO("Updated lite_subscription_quota={}, bindTopic={}, group={}",
                lite_subscription_quota_, bind_topic_, group_.name());
  }
  if (subscription.has_max_lite_topic_size()) {
    max_lite_topic_size_ = subscription.max_lite_topic_size();
    SPDLOG_INFO("Updated max_lite_topic_size={}, bindTopic={}, group={}",
                max_lite_topic_size_, bind_topic_, group_.name());
  }
}

void LiteSubscriptionManager::onNotifyUnsubscribeLiteCommand(const std::string& lite_topic) {
  SPDLOG_INFO("NotifyUnsubscribeLiteCommand: liteTopic={}, group={}, bindTopic={}",
              lite_topic, group_.name(), bind_topic_);
  if (!lite_topic.empty()) {
    absl::MutexLock lk(&lite_topic_set_mtx_);
    lite_topic_set_.erase(lite_topic);
  }
}

void LiteSubscriptionManager::syncAllLiteSubscription() {
  std::set<std::string> topics;
  {
    absl::MutexLock lk(&lite_topic_set_mtx_);
    topics.insert(lite_topic_set_.begin(), lite_topic_set_.end());
  }

  // Check quota with 0 delta (just log warning)
  std::error_code ec;
  checkLiteSubscriptionQuota(0, ec);
  if (ec) {
    SPDLOG_WARN("Lite subscription quota exceeded during syncAll, bindTopic={}, group={}",
                bind_topic_, group_.name());
    return;
  }

  syncLiteSubscription(rmq::LiteSubscriptionAction::COMPLETE_ADD, topics, nullptr, ec);
  if (ec) {
    SPDLOG_ERROR("Schedule syncAllLiteSubscription failed, clientId={}, ec={}",
                 client_config_.client_id, ec.message());
  }
}

void LiteSubscriptionManager::syncLiteSubscription(rmq::LiteSubscriptionAction action,
                                                    const std::set<std::string>& topics,
                                                    const OffsetOption* offset_option,
                                                    std::error_code& ec) {
  SyncLiteSubscriptionRequest request;
  request.set_action(action);
  request.mutable_topic()->set_resource_namespace(client_config_.resource_namespace);
  request.mutable_topic()->set_name(bind_topic_);
  request.mutable_group()->CopyFrom(group_);
  for (const auto& topic : topics) {
    request.add_lite_topic_set(topic);
  }

  if (offset_option) {
    auto* proto_option = request.mutable_offset_option();
    switch (offset_option->type()) {
      case OffsetOption::Type::POLICY: {
        auto policy_value = static_cast<OffsetOption::Policy>(offset_option->value());
        switch (policy_value) {
          case OffsetOption::Policy::LAST:
            proto_option->set_policy(rmq::OffsetOption_Policy_LAST);
            break;
          case OffsetOption::Policy::MIN:
            proto_option->set_policy(rmq::OffsetOption_Policy_MIN);
            break;
          case OffsetOption::Policy::MAX:
            proto_option->set_policy(rmq::OffsetOption_Policy_MAX);
            break;
        }
        break;
      }
      case OffsetOption::Type::OFFSET:
        proto_option->set_offset(offset_option->value());
        break;
      case OffsetOption::Type::TAIL_N:
        proto_option->set_tail_n(offset_option->value());
        break;
      case OffsetOption::Type::TIMESTAMP:
        proto_option->set_timestamp(offset_option->value());
        break;
    }
  }

  SPDLOG_DEBUG("SyncLiteSubscription: action={}, bindTopic={}, group={}, topics-size={}",
               static_cast<int>(action), bind_topic_, group_.name(), topics.size());

  auto timeout = absl::ToChronoMilliseconds(client_config_.request_timeout);

  Metadata metadata;
  Signature::sign(client_config_, metadata);

  // Get endpoints from the provider (set by LitePushConsumerImpl)
  absl::flat_hash_set<std::string> endpoints;
  if (endpoints_provider_) {
    endpoints = endpoints_provider_();
  }

  if (endpoints.empty()) {
    SPDLOG_WARN("No endpoints available for SyncLiteSubscription, bindTopic={}, group={}",
                bind_topic_, group_.name());
    ec = ErrorCode::NotFound;
    return;
  }

  for (const auto& target_host : endpoints) {
    auto callback = [&ec](const std::error_code& callback_ec, const SyncLiteSubscriptionResponse& /*response*/) {
      if (callback_ec) {
        ec = callback_ec;
      }
    };
    client_manager_->syncLiteSubscription(target_host, metadata, request, timeout, callback);
  }
}

void LiteSubscriptionManager::validateLiteTopic(const std::string& lite_topic, int max_length, std::error_code& ec) {
  if (lite_topic.empty()) {
    SPDLOG_ERROR("liteTopic is blank");
    ec = ErrorCode::IllegalLiteTopic;
    return;
  }
  if (static_cast<int>(lite_topic.length()) > max_length) {
    SPDLOG_ERROR("liteTopic length {} exceeded max length {}, liteTopic: {}",
                 lite_topic.length(), max_length, lite_topic);
    ec = ErrorCode::IllegalLiteTopic;
    return;
  }
}

void LiteSubscriptionManager::checkLiteSubscriptionQuota(int delta, std::error_code& ec) {
  std::size_t current_size = 0;
  {
    absl::MutexLock lk(&lite_topic_set_mtx_);
    current_size = lite_topic_set_.size();
  }

  if (static_cast<std::int32_t>(current_size) + delta > lite_subscription_quota_) {
    SPDLOG_WARN("Lite subscription quota exceeded: current={}, delta={}, quota={}",
                current_size, delta, lite_subscription_quota_);
    ec = ErrorCode::LiteSubscriptionQuotaExceeded;
  }
}

ROCKETMQ_NAMESPACE_END
