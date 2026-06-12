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
#include "LitePushConsumerImpl.h"

#include <chrono>
#include <memory>
#include <set>
#include <string>
#include <system_error>

#include "MixAll.h"
#include "Protocol.h"
#include "rocketmq/ErrorCode.h"
#include "rocketmq/OffsetOption.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

LitePushConsumerImpl::LitePushConsumerImpl(absl::string_view group_name, const std::string& bind_topic)
    : ClientImpl(group_name), PushConsumerImpl(group_name), bind_topic_(bind_topic) {
  // Subscribe to bind topic with SUB_ALL expression
  subscribe(bind_topic_, "*", ExpressionType::TAG);
}

LitePushConsumerImpl::~LitePushConsumerImpl() {
  shutdown();
}

void LitePushConsumerImpl::start() {
  // Call parent start
  PushConsumerImpl::start();

  // Create LiteSubscriptionManager lazily here (client_manager_ is ready after parent start)
  lite_subscription_manager_ = std::make_unique<LiteSubscriptionManager>(
      client_manager_, client_config_, bind_topic_, client_config_.subscriber.group);

  // Set endpoints provider so LiteSubscriptionManager can get current route endpoints
  lite_subscription_manager_->setEndpointsProvider([this]() -> absl::flat_hash_set<std::string> {
    return getCurrentEndpoints();
  });

  // Start LiteSubscriptionManager (initial full sync + periodic sync)
  lite_subscription_manager_->startUp();

  SPDLOG_INFO("LitePushConsumer started, group={}, bindTopic={}",
              client_config_.subscriber.group.name(), bind_topic_);
}

void LitePushConsumerImpl::shutdown() {
  State expecting = State::STARTED;
  if (state_.compare_exchange_strong(expecting, State::STOPPING)) {
    // Stop LiteSubscriptionManager first
    if (lite_subscription_manager_) {
      lite_subscription_manager_->shutdown();
    }

    // Then call parent shutdown (this will transition state to STOPPED)
    // Note: PushConsumerImpl::shutdown() expects STARTED state, but we already
    // changed to STOPPING, so we call ClientImpl::shutdown() directly.
    ClientImpl::shutdown();

    SPDLOG_INFO("LitePushConsumer stopped, group={}, bindTopic={}",
                client_config_.subscriber.group.name(), bind_topic_);
  }
}

void LitePushConsumerImpl::buildClientSettings(rmq::Settings& settings) {
  // Use LITE_PUSH_CONSUMER type instead of PUSH_CONSUMER
  settings.set_client_type(rmq::ClientType::LITE_PUSH_CONSUMER);

  auto subscription = settings.mutable_subscription();
  subscription->mutable_group()->CopyFrom(client_config_.subscriber.group);

  auto polling_timeout_ms = absl::ToInt64Milliseconds(client_config_.subscriber.polling_timeout);
  subscription->mutable_long_polling_timeout()->set_seconds(polling_timeout_ms / 1000);
  subscription->mutable_long_polling_timeout()->set_nanos((polling_timeout_ms % 1000) * 1000000);
  subscription->set_receive_batch_size(client_config_.subscriber.receive_batch_size);

  // Add bind topic subscription
  {
    absl::MutexLock lk(&topic_filter_expression_table_mtx_);
    for (const auto& entry : topic_filter_expression_table_) {
      auto subscription_entry = new rmq::SubscriptionEntry;
      subscription_entry->mutable_topic()->set_resource_namespace(resourceNamespace());
      subscription_entry->mutable_topic()->set_name(entry.first);

      subscription_entry->mutable_expression()->set_expression(entry.second.content_);
      switch (entry.second.type_) {
        case ExpressionType::TAG: {
          subscription_entry->mutable_expression()->set_type(rmq::FilterType::TAG);
          break;
        }
        case ExpressionType::SQL92: {
          subscription_entry->mutable_expression()->set_type(rmq::FilterType::SQL);
          break;
        }
      }
      subscription->mutable_subscriptions()->AddAllocated(subscription_entry);
    }
  }
}

void LitePushConsumerImpl::prepareHeartbeatData(HeartbeatRequest& request) {
  // Use LITE_PUSH_CONSUMER type
  request.set_client_type(rmq::ClientType::LITE_PUSH_CONSUMER);
  request.mutable_group()->CopyFrom(client_config_.subscriber.group);
}

void LitePushConsumerImpl::subscribeLite(const std::string& lite_topic,
                                         const OffsetOption* offset_option,
                                         std::error_code& ec) {
  if (!lite_subscription_manager_) {
    SPDLOG_ERROR("LiteSubscriptionManager is not initialized");
    ec = ErrorCode::IllegalState;
    return;
  }
  lite_subscription_manager_->subscribeLite(lite_topic, offset_option, ec);
}

void LitePushConsumerImpl::unsubscribeLite(const std::string& lite_topic, std::error_code& ec) {
  if (!lite_subscription_manager_) {
    SPDLOG_ERROR("LiteSubscriptionManager is not initialized");
    ec = ErrorCode::IllegalState;
    return;
  }
  lite_subscription_manager_->unsubscribeLite(lite_topic, ec);
}

std::set<std::string> LitePushConsumerImpl::getLiteTopicSet() const {
  if (!lite_subscription_manager_) {
    return {};
  }
  return lite_subscription_manager_->getLiteTopicSet();
}

void LitePushConsumerImpl::onNotifyUnsubscribeLiteCommand(const std::string& lite_topic) {
  SPDLOG_INFO("LitePushConsumer received NotifyUnsubscribeLiteCommand: liteTopic={}, group={}, bindTopic={}",
              lite_topic, client_config_.subscriber.group.name(), bind_topic_);
  if (lite_subscription_manager_) {
    lite_subscription_manager_->onNotifyUnsubscribeLiteCommand(lite_topic);
  }
}

void LitePushConsumerImpl::onSettingsUpdate(const rmq::Settings& settings) {
  if (lite_subscription_manager_) {
    lite_subscription_manager_->syncSettings(settings);
  }
}

absl::flat_hash_set<std::string> LitePushConsumerImpl::getCurrentEndpoints() {
  absl::flat_hash_set<std::string> endpoints;
  endpointsInUse(endpoints);
  return endpoints;
}

ROCKETMQ_NAMESPACE_END
