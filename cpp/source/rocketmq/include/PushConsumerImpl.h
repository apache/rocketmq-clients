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
#include <mutex>
#include <string>
#include <system_error>

#include "ClientConfig.h"
#include "ClientImpl.h"
#include "ClientManagerImpl.h"
#include "ConsumeMessageService.h"
#include "ConsumeStats.h"
#include "ProcessQueue.h"
#include "Scheduler.h"
#include "TopicAssignmentInfo.h"
#include "TopicPublishInfo.h"
#include "UtilAll.h"
#include "absl/strings/string_view.h"
#include "rocketmq/Executor.h"
#include "rocketmq/FilterExpression.h"
#include "rocketmq/State.h"

ROCKETMQ_NAMESPACE_BEGIN

class ConsumeMessageService;
class ConsumeFifoMessageService;
class ConsumeStandardMessageService;

class PushConsumerImpl : virtual public ClientImpl, public std::enable_shared_from_this<PushConsumerImpl> {
public:
  explicit PushConsumerImpl(absl::string_view group_name);

  ~PushConsumerImpl() override;

  void buildClientSettings(rmq::Settings& settings) override LOCKS_EXCLUDED(topic_filter_expression_table_mtx_);

  void prepareHeartbeatData(HeartbeatRequest& request) override;

  void topicsOfInterest(std::vector<std::string> &topics) override LOCKS_EXCLUDED(topic_filter_expression_table_mtx_);

  void start() override;

  void shutdown() override;

  void subscribe(const std::string& topic,
                 const std::string& expression,
                 ExpressionType expression_type = ExpressionType::TAG)
      LOCKS_EXCLUDED(topic_filter_expression_table_mtx_);

  void unsubscribe(const std::string& topic) LOCKS_EXCLUDED(topic_filter_expression_table_mtx_);

  absl::optional<FilterExpression> getFilterExpression(const std::string& topic) const
      LOCKS_EXCLUDED(topic_filter_expression_table_mtx_);

  void registerMessageListener(MessageListener message_listener);

  void scanAssignments() LOCKS_EXCLUDED(topic_filter_expression_table_mtx_);

  bool selectBroker(const TopicRouteDataPtr& route, std::string& broker_host);

  void wrapQueryAssignmentRequest(const std::string& topic,
                                  const std::string& consumer_group,
                                  const std::string& strategy_name,
                                  QueryAssignmentRequest& request);

  /**
   * Query assignment of the specified topic from load balancer directly if
   * message consuming mode is clustering. In case current client is operating
   * in the broadcasting mode, assignments are constructed locally from topic
   * route entries.
   *
   * @param topic Topic to query
   * @return shared pointer to topic assignment info
   */
  void queryAssignment(const std::string& topic,
                       const std::function<void(const std::error_code&, const TopicAssignmentPtr&)>& cb);

  void syncProcessQueue(const std::string& topic,
                        const TopicAssignmentPtr& topic_assignment,
                        const FilterExpression& filter_expression) LOCKS_EXCLUDED(process_queue_table_mtx_);

  std::shared_ptr<ProcessQueue> getOrCreateProcessQueue(const rmq::MessageQueue& message_queue,
                                                        const FilterExpression& filter_expression)
      LOCKS_EXCLUDED(process_queue_table_mtx_);

  bool receiveMessage(const rmq::MessageQueue& message_queue,
                      const FilterExpression& filter_expression,
                      std::string& attempt_id) LOCKS_EXCLUDED(process_queue_table_mtx_);

  uint32_t consumeThreadPoolSize() const;

  void consumeThreadPoolSize(int thread_pool_size);

  int32_t maxDeliveryAttempts() const {
    return max_delivery_attempts_;
  }

  int32_t receiveBatchSize() const {
    return receive_batch_size_;
  }

  std::shared_ptr<ConsumeMessageService> getConsumeMessageService();

  void ack(const Message& msg, const std::function<void(const std::error_code&)>& callback);

  void nack(const Message& message, const std::function<void(const std::error_code&)>& callback);

  void forwardToDeadLetterQueue(const Message& message, const std::function<void(const std::error_code&)>& cb);

  void wrapAckMessageRequest(const Message& msg, AckMessageRequest& request);

  // only for test
  std::size_t getProcessQueueTableSize() LOCKS_EXCLUDED(process_queue_table_mtx_);

  void setCustomExecutor(const Executor& executor) {
    custom_executor_ = executor;
  }

  const Executor& customExecutor() const {
    return custom_executor_;
  }

  void setThrottle(const std::string& topic, uint32_t threshold);

  /**
   * Max number of messages that may be cached per queue before applying
   * back-pressure.
   * @return
   */
  uint32_t maxCachedMessageQuantity() const {
    return MixAll::DEFAULT_CACHED_MESSAGE_COUNT;
  }

  /**
   * Threshold of total cached message body size by queue before applying
   * back-pressure.
   * @return
   */
  uint64_t maxCachedMessageMemory() const {
    return MixAll::DEFAULT_CACHED_MESSAGE_MEMORY;
  }

  MessageListener& messageListener() {
    return message_listener_;
  }

  const std::string& groupName() const {
    return client_config_.subscriber.group.name();
  }

  const ConsumeStats& stats() const {
    return stats_;
  }

protected:
  std::shared_ptr<ClientImpl> self() override {
    return shared_from_this();
  }

  void notifyClientTermination() override;

  void onVerifyMessage(MessageConstSharedPtr, std::function<void(TelemetryCommand)>) override;

private:
  absl::flat_hash_map<std::string, FilterExpression> topic_filter_expression_table_
      GUARDED_BY(topic_filter_expression_table_mtx_);
  mutable absl::Mutex topic_filter_expression_table_mtx_;

  /**
   * Consume message thread pool size.
   */
  uint32_t consume_thread_pool_size_{MixAll::DEFAULT_CONSUME_THREAD_POOL_SIZE};

  MessageListener message_listener_;

  std::shared_ptr<ConsumeMessageService> consume_message_service_;
  uint32_t consume_batch_size_{MixAll::DEFAULT_CONSUME_MESSAGE_BATCH_SIZE};

  int32_t receive_batch_size_{MixAll::DEFAULT_RECEIVE_MESSAGE_BATCH_SIZE};

  std::uintptr_t scan_assignment_handle_{0};
  static const char* SCAN_ASSIGNMENT_TASK_NAME;

  /**
   * simple-queue-name ==> process-queue
   */
  absl::flat_hash_map<std::string, std::shared_ptr<ProcessQueue>> process_queue_table_
      GUARDED_BY(process_queue_table_mtx_);
  absl::Mutex process_queue_table_mtx_;

  Executor custom_executor_;

  absl::flat_hash_map<std::string /* Topic */, uint32_t /* Threshold */> throttle_table_
      GUARDED_BY(throttle_table_mtx_);
  absl::Mutex throttle_table_mtx_;

  int32_t max_delivery_attempts_{MixAll::DEFAULT_MAX_DELIVERY_ATTEMPTS};

  ConsumeStats stats_;
  std::uintptr_t collect_stats_handle_{0};
  static const char* COLLECT_STATS_TASK_NAME;

  void fetchRoutes() LOCKS_EXCLUDED(topic_filter_expression_table_mtx_);

  void collectCacheStats() LOCKS_EXCLUDED(process_queue_table_mtx_);

  friend class ConsumeMessageService;
  friend class ConsumeFifoMessageService;
  friend class ConsumeStandardMessageService;
};

ROCKETMQ_NAMESPACE_END