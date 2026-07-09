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
#include <memory>
#include <set>
#include <string>
#include <system_error>

#include "Configuration.h"
#include "Executor.h"
#include "FilterExpression.h"
#include "Logger.h"
#include "MessageListener.h"
#include "OffsetOption.h"

ROCKETMQ_NAMESPACE_BEGIN

class LitePushConsumerImpl;
class LitePushConsumerBuilder;

/**
 * @brief LitePushConsumer provides a lightweight push-based message consumption model.
 *
 * Unlike PushConsumer which subscribes to fixed topics at build time, LitePushConsumer
 * binds to a parent topic and allows dynamic subscription/unsubscription of lite topics
 * at runtime through subscribeLite() and unsubscribeLite().
 *
 * Key features:
 * - Dynamic lite topic subscription and unsubscription
 * - Server-side quota management for lite subscriptions
 * - Support for FIFO and standard consumption modes
 * - Automatic re-synchronization of lite subscriptions every 30 seconds
 */
class LitePushConsumer {
public:
  static LitePushConsumerBuilder newBuilder();

  /**
   * Subscribe to a lite topic with default offset (LAST).
   *
   * @param lite_topic Name of the lite topic to subscribe to.
   * @param ec Error code set on failure (e.g., quota exceeded, network error).
   */
  void subscribeLite(const std::string& lite_topic, std::error_code& ec);

  /**
   * Subscribe to a lite topic with a specific offset option.
   *
   * @param lite_topic Name of the lite topic to subscribe to.
   * @param offset_option Specifies where to start consuming.
   * @param ec Error code set on failure.
   */
  void subscribeLite(const std::string& lite_topic, const OffsetOption& offset_option, std::error_code& ec);

  /**
   * Unsubscribe from a lite topic.
   *
   * @param lite_topic Name of the lite topic to unsubscribe from.
   * @param ec Error code set on failure.
   */
  void unsubscribeLite(const std::string& lite_topic, std::error_code& ec);

  /**
   * Get the set of currently subscribed lite topics.
   *
   * @return Immutable copy of lite topic set.
   */
  std::set<std::string> getLiteTopicSet() const;

  /**
   * Get the consumer group name.
   *
   * @return Consumer group name.
   */
  std::string getConsumerGroup() const;

  /**
   * Shutdown the consumer and release all resources.
   */
  void shutdown();

private:
  friend class LitePushConsumerBuilder;

  explicit LitePushConsumer(std::shared_ptr<LitePushConsumerImpl> impl)
      : impl_(std::move(impl)) {
  }

  std::shared_ptr<LitePushConsumerImpl> impl_;
};

/**
 * @brief Builder for constructing LitePushConsumer instances.
 */
class LitePushConsumerBuilder {
public:
  LitePushConsumerBuilder() : configuration_(Configuration::newBuilder().build()) {}

  /**
   * Set the bind topic for the lite push consumer.
   * This is the parent topic that all lite topics belong to.
   *
   * @param topic Parent topic name.
   */
  LitePushConsumerBuilder& bindTopic(std::string topic) {
    bind_topic_ = std::move(topic);
    return *this;
  }

  /**
   * Set client configuration (endpoints, credentials, etc.).
   */
  LitePushConsumerBuilder& withConfiguration(Configuration configuration) {
    configuration_ = std::move(configuration);
    return *this;
  }

  /**
   * Set the consumer group name for load balancing.
   */
  LitePushConsumerBuilder& withGroup(std::string group) {
    group_ = std::move(group);
    return *this;
  }

  /**
   * Register the message listener to process received messages.
   */
  LitePushConsumerBuilder& withListener(MessageListener listener) {
    listener_ = std::move(listener);
    return *this;
  }

  /**
   * Set the maximum number of messages cached locally.
   *
   * @param count Maximum cached message count (must be > 0). Default: 1024.
   */
  LitePushConsumerBuilder& withMaxCacheMessageCount(int count) {
    max_cache_message_count_ = count;
    return *this;
  }

  /**
   * Set the maximum bytes of messages cached locally.
   *
   * @param bytes Maximum cached message bytes (must be > 0). Default: 64MB.
   */
  LitePushConsumerBuilder& withMaxCacheMessageSizeInBytes(int bytes) {
    max_cache_message_size_in_bytes_ = bytes;
    return *this;
  }

  /**
   * Set the number of consumption threads.
   *
   * @param count Thread count (must be > 0). Default: 20.
   */
  LitePushConsumerBuilder& withConsumeThreads(int count) {
    consume_threads_ = count;
    return *this;
  }

  /**
   * Enable FIFO consume accelerator for parallel processing by lite topic.
   * May increase the probability of repeated message consumption.
   */
  LitePushConsumerBuilder& withFifoConsumeAccelerator(bool enable) {
    fifo_consume_accelerator_ = enable;
    return *this;
  }

  /**
   * Build and start the LitePushConsumer.
   *
   * This method blocks until the consumer starts successfully.
   *
   * @return A started LitePushConsumer instance.
   * @throws std::invalid_argument if required parameters are missing.
   */
  LitePushConsumer build();

private:
  std::string bind_topic_;
  std::string group_;
  Configuration configuration_;
  MessageListener listener_;
  int max_cache_message_count_{1024};
  int max_cache_message_size_in_bytes_{64 * 1024 * 1024};
  int consume_threads_{20};
  bool fifo_consume_accelerator_{false};
};

ROCKETMQ_NAMESPACE_END
