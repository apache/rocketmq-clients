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
#include "PushConsumerImpl.h"

#include <cassert>
#include <chrono>
#include <cstdlib>
#include <string>
#include <system_error>

#include "AsyncReceiveMessageCallback.h"
#include "ConsumeMessageServiceImpl.h"
#include "MixAll.h"
#include "ProcessQueueImpl.h"
#include "Protocol.h"
#include "RpcClient.h"
#include "Signature.h"
#include "Tag.h"
#include "google/protobuf/util/time_util.h"
#include "opencensus/stats/stats.h"
#include "rocketmq/MQClientException.h"
#include "rocketmq/MessageListener.h"

ROCKETMQ_NAMESPACE_BEGIN

PushConsumerImpl::PushConsumerImpl(absl::string_view group_name) : ClientImpl(group_name) {
}

PushConsumerImpl::~PushConsumerImpl() {
  SPDLOG_DEBUG("DefaultMQPushConsumerImpl is destructed");
  shutdown();
}

void PushConsumerImpl::topicsOfInterest(std::vector<std::string> &topics) {
  absl::MutexLock lk(&topic_filter_expression_table_mtx_);
  for (const auto& entry : topic_filter_expression_table_) {
    topics.push_back(entry.first);
  }
}

void PushConsumerImpl::start() {
  ClientImpl::start();

  State expecting = State::STARTING;
  if (!state_.compare_exchange_strong(expecting, State::STARTED)) {
    SPDLOG_ERROR("Unexpected consumer state. Expecting: {}, Actual: {}", State::STARTING,
                 state_.load(std::memory_order_relaxed));
    return;
  }

  if (!message_listener_) {
    SPDLOG_ERROR("Required message listener is missing");
    abort();
  }

  client_config_.subscriber.group.set_resource_namespace(resourceNamespace());
  client_manager_->addClientObserver(shared_from_this());

  fetchRoutes();

  SPDLOG_INFO("Start concurrently consume service: {}", client_config_.subscriber.group.name());
  consume_message_service_ = std::make_shared<ConsumeMessageServiceImpl>(
      shared_from_this(), consume_thread_pool_size_, message_listener_);
  consume_message_service_->start();

  // Heartbeat depends on initialization of consume-message-service
  heartbeat();

  std::weak_ptr<PushConsumerImpl> consumer_weak_ptr(shared_from_this());
  auto scan_assignment_functor = [consumer_weak_ptr]() {
    std::shared_ptr<PushConsumerImpl> consumer = consumer_weak_ptr.lock();
    if (consumer) {
      consumer->scanAssignments();
    }
  };

  scan_assignment_handle_ = client_manager_->getScheduler()->schedule(
      scan_assignment_functor, SCAN_ASSIGNMENT_TASK_NAME,
      std::chrono::milliseconds(100), std::chrono::seconds(5));
  SPDLOG_INFO("PushConsumer started, groupName={}", client_config_.subscriber.group.name());

  auto collect_stats_functor = [consumer_weak_ptr] {
    auto consumer = consumer_weak_ptr.lock();
    if (consumer) {
      consumer->collectCacheStats();
    }
  };

  collect_stats_handle_ = client_manager_->getScheduler()->schedule(
      collect_stats_functor, COLLECT_STATS_TASK_NAME,
      std::chrono::seconds(3), std::chrono::seconds(3));
}

const char* PushConsumerImpl::SCAN_ASSIGNMENT_TASK_NAME = "scan-assignment-task";
const char* PushConsumerImpl::COLLECT_STATS_TASK_NAME = "collect-stats-task";

void PushConsumerImpl::shutdown() {
  State expecting = State::STARTED;
  if (state_.compare_exchange_strong(expecting, State::STOPPING)) {
    if (scan_assignment_handle_) {
      client_manager_->getScheduler()->cancel(scan_assignment_handle_);
      SPDLOG_DEBUG("Scan assignment periodic task cancelled");
    }

    if (collect_stats_handle_) {
      client_manager_->getScheduler()->cancel(collect_stats_handle_);
      SPDLOG_DEBUG("Collect cache stats periodic task cancelled");
    }

    {
      absl::MutexLock lock(&process_queue_table_mtx_);
      process_queue_table_.clear();
    }

    if (consume_message_service_) {
      consume_message_service_->shutdown();
    }

    // Shutdown services started by parent
    ClientImpl::shutdown();

    SPDLOG_INFO("PushConsumerImpl stopped");
  } else {
    SPDLOG_ERROR("Shutdown with unexpected state. Expecting: {}, Actual: {}", State::STARTED,
                 state_.load(std::memory_order_relaxed));
  }
}

void PushConsumerImpl::subscribe(const std::string& topic, const std::string& expression,
                                 ExpressionType expression_type) {
  absl::MutexLock lock(&topic_filter_expression_table_mtx_);
  FilterExpression filter_expression{expression, expression_type};
  topic_filter_expression_table_.emplace(topic, filter_expression);
}

void PushConsumerImpl::unsubscribe(const std::string& topic) {
  absl::MutexLock lock(&topic_filter_expression_table_mtx_);
  topic_filter_expression_table_.erase(topic);
}

absl::optional<FilterExpression> PushConsumerImpl::getFilterExpression(const std::string& topic) const {
  {
    absl::MutexLock lock(&topic_filter_expression_table_mtx_);
    if (topic_filter_expression_table_.contains(topic)) {
      return absl::make_optional(topic_filter_expression_table_.at(topic));
    } else {
      return {};
    }
  }
}

void PushConsumerImpl::scanAssignments() {
  SPDLOG_DEBUG("Start of assignment scanning");
  if (!active()) {
    SPDLOG_INFO("Client has stopped. Abort scanning immediately.");
    return;
  }

  {
    absl::MutexLock lk(&topic_filter_expression_table_mtx_);
    for (auto& entry : topic_filter_expression_table_) {
      std::string topic = entry.first;
      const auto& filter_expression = entry.second;
      SPDLOG_DEBUG("Scan assignments for {}", topic);
      auto callback = [this, topic, filter_expression](const std::error_code& ec,
                                                       const TopicAssignmentPtr& assignments) {
        if (ec) {
          SPDLOG_WARN("Failed to acquire assignments for topic={} from load balancer. Cause: {}", topic, ec.message());
        } else if (assignments && !assignments->assignmentList().empty()) {
          syncProcessQueue(topic, assignments, filter_expression);
        }
      };
      queryAssignment(topic, callback);
    } // end of for-loop
  }
  SPDLOG_DEBUG("End of assignment scanning.");
}

bool PushConsumerImpl::selectBroker(const TopicRouteDataPtr& topic_route_data, std::string& broker_host) {

  absl::flat_hash_set<std::string> endpoints;
  endpointsInUse(endpoints);
  if (endpoints.empty()) {
    SPDLOG_WARN("No broker is available");
    return false;
  }

  // preference for selecting the access point filled in by the user
  if (topic_route_data && !topic_route_data->messageQueues().empty()) {
    uint32_t queue_count = topic_route_data->messageQueues().size();
    uint32_t index = TopicAssignment::getAndIncreaseQueryWhichBroker();
    for (uint32_t i = index; i < index + queue_count; i++) {
      auto message_queue = topic_route_data->messageQueues().at(i % queue_count);
      if (MixAll::MASTER_BROKER_ID != message_queue.broker().id() || !readable(message_queue.permission())) {
        continue;
      }

      std::string current_host = urlOf(message_queue);
      if (endpoints.contains(current_host)) {
        broker_host = current_host;
        return true;
      }
    }

    for (uint32_t i = index; i < index + queue_count; i++) {
      auto message_queue = topic_route_data->messageQueues().at(i % queue_count);
      if (MixAll::MASTER_BROKER_ID != message_queue.broker().id() || !readable(message_queue.permission())) {
        continue;
      }
      broker_host = urlOf(message_queue);
      return true;
    }
  }
  return false;
}

void PushConsumerImpl::wrapQueryAssignmentRequest(const std::string&      topic,
                                                  const std::string&      consumer_group,
                                                  const std::string&      strategy_name,
                                                  QueryAssignmentRequest& request) {
  request.mutable_endpoints()->CopyFrom(accessPoint());
  request.mutable_topic()->set_name(topic);
  request.mutable_topic()->set_resource_namespace(resourceNamespace());
  request.mutable_group()->set_name(consumer_group);
  request.mutable_group()->set_resource_namespace(resourceNamespace());
}

void PushConsumerImpl::queryAssignment(
    const std::string& topic, const std::function<void(const std::error_code&, const TopicAssignmentPtr&)>& cb) {
  auto callback = [this, topic, cb](const std::error_code& ec, const TopicRouteDataPtr& topic_route) {
    TopicAssignmentPtr topic_assignment;
    std::string broker_host;
    if (!selectBroker(topic_route, broker_host)) {
      SPDLOG_WARN("Failed to select a broker to query assignment for group={}, topic={}",
                  client_config_.subscriber.group.name(), topic);
    }

    QueryAssignmentRequest request;
    wrapQueryAssignmentRequest(topic, groupName(), MixAll::DEFAULT_LOAD_BALANCER_STRATEGY_NAME_, request);
    SPDLOG_DEBUG("QueryAssignmentRequest: {}", request.DebugString());

    absl::flat_hash_map<std::string, std::string> metadata;
    Signature::sign(client_config_, metadata);
    auto assignment_callback = [this, cb, topic, broker_host](const std::error_code& ec,
                                                        const QueryAssignmentResponse& response) {
      if (ec) {
        SPDLOG_WARN("Failed to acquire queue assignment of topic={} from brokerAddress={}", topic, broker_host);
        cb(ec, nullptr);
      } else {
        SPDLOG_DEBUG("Query topic assignment OK. Topic={}, group={}, assignment-size={}", topic, groupName(),
                     response.assignments().size());
        SPDLOG_TRACE("Query assignment response for {} is: {}", topic, response.DebugString());
        cb(ec, std::make_shared<TopicAssignment>(response));
      }
    };

    client_manager_->queryAssignment(broker_host, metadata, request,
                                     absl::ToChronoMilliseconds(client_config_.request_timeout), assignment_callback);
  };
  getRouteFor(topic, callback);
}

/**
 *
 * @param topic Topic to process
 * @param assignment Latest assignment from load balancer
 * @param filter_expression Filter expression
 */
void PushConsumerImpl::syncProcessQueue(const std::string& topic,
                                        const std::shared_ptr<TopicAssignment>& topic_assignment,
                                        const FilterExpression& filter_expression) {
  const std::vector<rmq::Assignment>& assignment_list = topic_assignment->assignmentList();
  std::vector<rmq::MessageQueue> message_queue_list;
  message_queue_list.reserve(assignment_list.size());
  for (const auto& assignment : assignment_list) {
    message_queue_list.push_back(assignment.message_queue());
  }

  std::vector<rmq::MessageQueue> current;
  {
    absl::MutexLock lock(&process_queue_table_mtx_);
    for (auto it = process_queue_table_.begin(); it != process_queue_table_.end();) {
      if (topic != it->second->messageQueue().topic().name()) {
        it++;
        continue;
      }

      if (std::none_of(
              message_queue_list.cbegin(), message_queue_list.cend(),
              [&](const rmq::MessageQueue& message_queue) { return it->second->messageQueue() == message_queue; })) {
        SPDLOG_INFO("Stop receiving messages from {} as it is not assigned to current client according to latest "
                    "assignment result from load balancer",
                    simpleNameOf(it->second->messageQueue()));
        process_queue_table_.erase(it++);
      } else {
        if (!it->second || it->second->expired()) {
          SPDLOG_WARN("ProcessQueue={} is expired. Remove it for now.", it->first);
          process_queue_table_.erase(it++);
          continue;
        }
        current.push_back(it->second->messageQueue());
        it++;
      }
    }
  }

  for (const auto& message_queue : message_queue_list) {
    if (std::none_of(current.cbegin(), current.cend(),
                     [&](const rmq::MessageQueue& item) { return item == message_queue; })) {
      SPDLOG_DEBUG("Start to receive message from {} according to latest assignment info from load balancer",
                  simpleNameOf(message_queue));
      std::string attempt_id;
      if (!receiveMessage(message_queue, filter_expression, attempt_id)) {
        if (!active()) {
          SPDLOG_WARN("Failed to initiate receive message request-response-cycle for {}", simpleNameOf(message_queue));
          // TODO: remove it from current assignment such that a second attempt will be made again in the next round.
        }
      }
    }
  }
}

std::shared_ptr<ProcessQueue> PushConsumerImpl::getOrCreateProcessQueue(const rmq::MessageQueue& message_queue,
                                                                        const FilterExpression& filter_expression) {
  std::shared_ptr<ProcessQueue> process_queue;
  {
    absl::MutexLock lock(&process_queue_table_mtx_);
    if (!active()) {
      SPDLOG_INFO("PushConsumer has stopped. Drop creation of ProcessQueue");
      return process_queue;
    }

    if (process_queue_table_.contains(simpleNameOf(message_queue))) {
      process_queue = process_queue_table_.at(simpleNameOf(message_queue));
    } else {
      SPDLOG_INFO("Create ProcessQueue for message queue[{}]", simpleNameOf(message_queue));
      // create process queue object
      process_queue = std::make_shared<ProcessQueueImpl>(
          message_queue, filter_expression, shared_from_this(), client_manager_);
      std::shared_ptr<AsyncReceiveMessageCallback> receive_callback =
          std::make_shared<AsyncReceiveMessageCallback>(process_queue);
      process_queue->callback(receive_callback);
      process_queue_table_.emplace(std::make_pair(simpleNameOf(message_queue), process_queue));
    }
  }
  return process_queue;
}

bool PushConsumerImpl::receiveMessage(const rmq::MessageQueue& message_queue,
                                      const FilterExpression& filter_expression,
                                      std::string& attempt_id) {
  if (!active()) {
    SPDLOG_INFO("PushConsumer has stopped. Drop further receive message request");
    return false;
  }

  auto process_queue_ptr = getOrCreateProcessQueue(message_queue, filter_expression);
  if (!process_queue_ptr) {
    SPDLOG_INFO("Consumer has stopped. Stop creating processQueue");
    return false;
  }
  const std::string& broker_host = urlOf(message_queue);
  if (broker_host.empty()) {
    SPDLOG_ERROR("Failed to resolve address for brokerName={}", message_queue.broker().name());
    return false;
  }
  process_queue_ptr->receiveMessage(attempt_id);
  return true;
}

std::shared_ptr<ConsumeMessageService> PushConsumerImpl::getConsumeMessageService() {
  return consume_message_service_;
}

void PushConsumerImpl::ack(const Message& msg, const std::function<void(const std::error_code&)>& callback) {
  const std::string& target_host = msg.extension().target_endpoint;
  assert(!target_host.empty());
  SPDLOG_DEBUG("Prepare to send ack to broker. BrokerAddress={}, topic={}, queueId={}, msgId={}", target_host,
               msg.topic(), msg.extension().queue_id, msg.id());
  AckMessageRequest request;
  wrapAckMessageRequest(msg, request);
  absl::flat_hash_map<std::string, std::string> metadata;
  Signature::sign(client_config_, metadata);
  client_manager_->ack(target_host, metadata, request, absl::ToChronoMilliseconds(client_config_.request_timeout),
                       callback);
}

void PushConsumerImpl::nack(const Message& message, const std::function<void(const std::error_code&)>& callback) {
  const auto& target_host = message.extension().target_endpoint;
  SPDLOG_DEBUG("Prepare to nack message[topic={}, message-id={}]", message.topic(), message.id());
  auto duration = backoff(message.extension().delivery_attempt + 1);
  Metadata metadata;
  Signature::sign(client_config_, metadata);
  rmq::ChangeInvisibleDurationRequest request;
  request.mutable_group()->CopyFrom(client_config_.subscriber.group);
  request.mutable_topic()->set_resource_namespace(resourceNamespace());
  request.mutable_topic()->set_name(message.topic());
  request.set_receipt_handle(message.extension().receipt_handle);
  request.set_message_id(message.id());
  request.mutable_invisible_duration()->CopyFrom(
      google::protobuf::util::TimeUtil::MillisecondsToDuration(duration.count()));

  auto cb =
      [callback](const std::error_code& ec, const ChangeInvisibleDurationResponse& response) {
        callback(ec);
      };
  client_manager_->changeInvisibleDuration(target_host, metadata, request,
                                           absl::ToChronoMilliseconds(client_config_.request_timeout), cb);
}

void PushConsumerImpl::forwardToDeadLetterQueue(const Message& message,
                                                const std::function<void(const std::error_code&)>& cb) {
  std::string target_host = message.extension().target_endpoint;

  absl::flat_hash_map<std::string, std::string> metadata;
  Signature::sign(client_config_, metadata);

  ForwardMessageToDeadLetterQueueRequest request;
  request.mutable_group()->set_resource_namespace(resourceNamespace());
  request.mutable_group()->set_name(groupName());

  request.mutable_topic()->set_resource_namespace(resourceNamespace());
  request.mutable_topic()->set_name(message.topic());

  request.set_message_id(message.id());

  request.set_delivery_attempt(message.extension().delivery_attempt);
  request.set_max_delivery_attempts(max_delivery_attempts_);

  client_manager_->forwardMessageToDeadLetterQueue(target_host, metadata, request,
                                                   absl::ToChronoMilliseconds(client_config_.request_timeout), cb);
}

void PushConsumerImpl::wrapAckMessageRequest(const Message& msg, AckMessageRequest& request) {
  request.mutable_group()->set_resource_namespace(resourceNamespace());
  request.mutable_group()->set_name(groupName());
  request.mutable_topic()->set_resource_namespace(resourceNamespace());
  request.mutable_topic()->set_name(msg.topic());
  auto entry = new rmq::AckMessageEntry();
  entry->set_message_id(msg.id());
  entry->set_receipt_handle(msg.extension().receipt_handle);
  request.mutable_entries()->AddAllocated(entry);
}

uint32_t PushConsumerImpl::consumeThreadPoolSize() const {
  return consume_thread_pool_size_;
}

void PushConsumerImpl::consumeThreadPoolSize(int thread_pool_size) {
  if (thread_pool_size >= 1) {
    consume_thread_pool_size_ = thread_pool_size;
  }
}

void PushConsumerImpl::registerMessageListener(MessageListener message_listener) {
  message_listener_ = std::move(message_listener);
}

std::size_t PushConsumerImpl::getProcessQueueTableSize() {
  absl::MutexLock lock(&process_queue_table_mtx_);
  return process_queue_table_.size();
}

void PushConsumerImpl::setThrottle(const std::string& topic, uint32_t threshold) {
  absl::MutexLock lock(&throttle_table_mtx_);
  throttle_table_.emplace(topic, threshold);
}

void PushConsumerImpl::fetchRoutes() {
  std::vector<std::string> topics;
  {
    absl::MutexLock lk(&topic_filter_expression_table_mtx_);
    for (const auto& item : topic_filter_expression_table_) {
      topics.emplace_back(item.first);
    }
  }

  if (topics.empty()) {
    return;
  }

  std::vector<std::string>::size_type countdown = topics.size();
  auto mtx = std::make_shared<absl::Mutex>();
  auto cv = std::make_shared<absl::CondVar>();
  int acquired = 0;
  auto callback = [&, mtx, cv](const std::error_code& ec, const TopicRouteDataPtr& route) {
    {
      absl::MutexLock lk(mtx.get());
      countdown--;
      if (!ec) {
        acquired++;
      }
    }

    if (countdown <= 0) {
      cv->SignalAll();
    }
  };

  for (const auto& topic : topics) {
    getRouteFor(topic, callback);
  }

  while (countdown) {
    absl::MutexLock lk(mtx.get());
    cv->Wait(mtx.get());
  }
  SPDLOG_INFO("Fetched route for {} out of {} topics", acquired, topics.size());
}

void PushConsumerImpl::buildClientSettings(rmq::Settings& settings) {
  settings.set_client_type(rmq::ClientType::PUSH_CONSUMER);
  auto subscription = settings.mutable_subscription();
  subscription->mutable_group()->CopyFrom(client_config_.subscriber.group);
  auto polling_timeout = google::protobuf::util::TimeUtil::MillisecondsToDuration(
      absl::ToInt64Milliseconds(client_config_.subscriber.polling_timeout));
  subscription->mutable_long_polling_timeout()->set_seconds(polling_timeout.seconds());
  subscription->mutable_long_polling_timeout()->set_nanos(polling_timeout.nanos());
  subscription->set_receive_batch_size(client_config_.subscriber.receive_batch_size);

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

void PushConsumerImpl::prepareHeartbeatData(HeartbeatRequest& request) {
  request.set_client_type(rmq::ClientType::PUSH_CONSUMER);
  request.mutable_group()->CopyFrom(client_config_.subscriber.group);
}

void PushConsumerImpl::notifyClientTermination() {
  NotifyClientTerminationRequest request;
  request.mutable_group()->set_resource_namespace(resourceNamespace());
  request.mutable_group()->set_name(groupName());
  ClientImpl::notifyClientTermination(request);
}

void PushConsumerImpl::onVerifyMessage(MessageConstSharedPtr message, std::function<void(TelemetryCommand)> cb) {
  auto listener = messageListener();
  TelemetryCommand cmd;
  auto verify_result = cmd.mutable_verify_message_result();
  verify_result->set_nonce(message->extension().nonce);
  if (message) {
    if (!client_config_.subscriber.fifo) {
      try {
        auto result = listener(*message);
        switch (result) {
          case ConsumeResult::SUCCESS: {
            cmd.mutable_status()->set_code(rmq::Code::OK);
            cmd.mutable_status()->set_message("OK");
          }
          case ConsumeResult::FAILURE: {
            cmd.mutable_status()->set_code(rmq::Code::FAILED_TO_CONSUME_MESSAGE);
            cmd.mutable_status()->set_message("Consume message failed");
          }
        }
      } catch (const std::exception& e) {
        cmd.mutable_status()->set_code(rmq::Code::FAILED_TO_CONSUME_MESSAGE);
        cmd.mutable_status()->set_message(e.what());
      } catch (...) {
        cmd.mutable_status()->set_code(rmq::Code::FAILED_TO_CONSUME_MESSAGE);
        cmd.mutable_status()->set_message("Unexpected exception raised");
      }
    } else {
      cmd.mutable_status()->set_code(rmq::Code::VERIFY_FIFO_MESSAGE_UNSUPPORTED);
      cmd.mutable_status()->set_message("Unsupported Operation For FIFO Message");
    }
  } else {
    cmd.mutable_status()->set_code(rmq::Code::MESSAGE_CORRUPTED);
    cmd.mutable_status()->set_message("Checksum Mismatch");
  }
}

void PushConsumerImpl::collectCacheStats() {
  absl::flat_hash_map<std::string, std::uint64_t> topic_count;
  absl::flat_hash_map<std::string, std::uint64_t> topic_memory;

  {
    absl::MutexLock lk(&process_queue_table_mtx_);
    for (const auto& entry : process_queue_table_) {
      auto&& topic = entry.second->topic();
      std::uint64_t cnt = entry.second->cachedMessageQuantity();
      std::uint64_t memory = entry.second->cachedMessageMemory();
      auto it = topic_count.find(topic);
      if (it == topic_count.end()) {
        topic_count.insert_or_assign(topic, cnt);
      } else {
        it->second += cnt;
      }

      it = topic_memory.find(topic);
      if (it == topic_memory.end()) {
        topic_memory.insert_or_assign(topic, memory);
      } else {
        it->second += memory;
      }
    }
  }

  for (const auto& entry : topic_count) {
    opencensus::stats::Record({{stats_.cachedMessageQuantity(), entry.second}},
                              {{Tag::topicTag(), entry.first}, {Tag::clientIdTag(), client_config_.client_id}});
    SPDLOG_DEBUG("Cache on Quantity {} --> {}", entry.first, entry.second);
  }

  for (const auto& entry : topic_memory) {
    opencensus::stats::Record({{stats_.cachedMessageBytes(), entry.second}},
                              {{Tag::topicTag(), entry.first}, {Tag::clientIdTag(), client_config_.client_id}});
    SPDLOG_DEBUG("Cache on Memory {} --> {}", entry.first, entry.second);
  }
}

ROCKETMQ_NAMESPACE_END
