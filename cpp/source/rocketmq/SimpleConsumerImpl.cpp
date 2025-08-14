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

#include "SimpleConsumerImpl.h"

#include "Signature.h"
#include "google/protobuf/util/time_util.h"
#include "rocketmq/ErrorCode.h"

ROCKETMQ_NAMESPACE_BEGIN

SimpleConsumerImpl::SimpleConsumerImpl(std::string group) : ClientImpl(group) {
  client_config_.subscriber.polling_timeout = absl::FromChrono(MixAll::DefaultReceiveMessageTimeout);
}

SimpleConsumerImpl::~SimpleConsumerImpl() {
  shutdown();
}

void SimpleConsumerImpl::prepareHeartbeatData(rmq::HeartbeatRequest& request) {
  request.set_client_type(rmq::ClientType::SIMPLE_CONSUMER);
  request.mutable_group()->CopyFrom(client_config_.subscriber.group);
}

void SimpleConsumerImpl::buildClientSettings(rmq::Settings& settings) {
  settings.set_client_type(rmq::ClientType::SIMPLE_CONSUMER);

  settings.mutable_subscription()->mutable_group()->CopyFrom(client_config_.subscriber.group);

  auto subscriptions = settings.mutable_subscription()->mutable_subscriptions();

  settings.mutable_access_point()->CopyFrom(accessPoint());
  {
    absl::MutexLock lk(&subscriptions_mtx_);
    for (const auto& entry : subscriptions_) {
      auto subscription_entry = new rmq::SubscriptionEntry;
      subscription_entry->mutable_topic()->set_name(entry.first);
      subscription_entry->mutable_topic()->set_resource_namespace(resourceNamespace());

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
      subscription_entry->mutable_expression()->set_expression(entry.second.content_);
      subscriptions->AddAllocated(subscription_entry);
    }
  }
}

void SimpleConsumerImpl::topicsOfInterest(std::vector<std::string> &topics) {
  absl::MutexLock lk(&subscriptions_mtx_);
  for (const auto& entry : subscriptions_) {
    if (std::find(topics.begin(), topics.end(), entry.first) == topics.end()) {
      topics.push_back(entry.first);
    }
  }
}

/**
 * @brief Start SimpleConsumer
 *
 * During start, we need synchronously fetch routes and query assignments
 */
void SimpleConsumerImpl::start() {
  ClientImpl::start();
  State expected = State::STARTING;
  if (state_.compare_exchange_strong(expected, State::STARTED, std::memory_order_relaxed)) {
    client_config_.subscriber.group.set_resource_namespace(resourceNamespace());
    refreshAssignments();

    std::weak_ptr<SimpleConsumerImpl> consumer(shared_from_this());
    auto refresh_assignment_task = [consumer]() {
      auto simple_consumer = consumer.lock();
      if (simple_consumer) {
        simple_consumer->refreshAssignments0();
      }
    };

    // refer java sdk: set refresh interval to 5 seconds
    // org.apache.rocketmq.client.java.impl.ClientImpl#startUp
    refresh_assignment_task_ = manager()->getScheduler()->schedule(
        refresh_assignment_task, "RefreshAssignmentTask",
        std::chrono::seconds(5), std::chrono::seconds(5));

    client_manager_->addClientObserver(shared_from_this());
  }
}

void SimpleConsumerImpl::shutdown() {
  State expected = State::STARTED;

  if (state_.compare_exchange_strong(expected, State::STOPPING, std::memory_order_relaxed)) {
    manager()->getScheduler()->cancel(refresh_assignment_task_);
    ClientImpl::shutdown();
  }
}

void SimpleConsumerImpl::subscribe(std::string topic, FilterExpression expression) {
  absl::MutexLock lk(&subscriptions_mtx_);
  subscriptions_.insert_or_assign(topic, expression);
}

void SimpleConsumerImpl::unsubscribe(const std::string& topic) {
  {
    absl::MutexLock lk(&subscriptions_mtx_);
    subscriptions_.erase(topic);
  }

  removeAssignmentsByTopic(topic);
}

void SimpleConsumerImpl::removeAssignmentsByTopic(const std::string& topic) {
  std::vector<rmq::Assignment> assignments;
  {
    absl::MutexLock lk(&topic_assignments_mtx_);
    if (!topic_assignments_.contains(topic)) {
      return;
    }

    const auto& items = topic_assignments_[topic];
    assignments.insert(assignments.end(), items.begin(), items.end());
    topic_assignments_.erase(topic);
  }

  {
    absl::MutexLock lk(&assignments_mtx_);
    auto it = std::remove_if(assignments_.begin(), assignments_.end(), [&](const rmq::Assignment& assignment) {
      return std::find_if(assignments.begin(), assignments.end(),
                          [&](const rmq::Assignment& e) { return e == assignment; }) != assignments.end();
    });
    assignments_.erase(it, assignments_.end());
  }
}

void SimpleConsumerImpl::refreshAssignments0() {
  std::vector<std::string> topics;
  {
    absl::MutexLock lk(&subscriptions_mtx_);
    for (const auto& entry : subscriptions_) {
      topics.push_back(entry.first);
    }
  }

  auto no_op = [](const std::error_code& ec) {};
  for (const auto& topic : topics) {
    refreshAssignment(topic, no_op);
  }
}

void SimpleConsumerImpl::updateAssignments(const std::string& topic, const std::vector<rmq::Assignment>& assignments) {
  bool changed = false;
  {
    absl::MutexLock lk(&topic_assignments_mtx_);
    if (!topic_assignments_.contains(topic)) {
      changed = true;
      topic_assignments_.insert({topic, assignments});
      {
        absl::MutexLock assignment_lk(&assignments_mtx_);
        assignments_.insert(assignments_.begin(), assignments.begin(), assignments.end());
      }
    } else if (!assignments.empty()) {
      const auto& prev = topic_assignments_[topic];
      std::vector<rmq::Assignment> to_remove;
      std::vector<rmq::Assignment> to_add;
      for (const auto& item : prev) {
        if (std::find_if(assignments.begin(), assignments.end(), [&](const rmq::Assignment& e) { return item == e; }) ==
            assignments.end()) {
          to_remove.push_back(item);
        }
      }

      for (const auto& entry : assignments) {
        if (std::find_if(prev.begin(), prev.end(), [&](const rmq::Assignment e) { return e == entry; }) == prev.end()) {
          to_add.push_back(entry);
        }
      }

      if (!to_remove.empty() || !to_add.empty()) {
        changed = true;
        absl::MutexLock lk(&assignments_mtx_);
        for (const auto& item : to_remove) {
          std::remove_if(assignments_.begin(), assignments_.end(), [&](const rmq::Assignment& e) { return e == item; });
        }

        for (const auto& item : to_add) {
          assignments_.push_back(item);
        }
        topic_assignments_.insert_or_assign(topic, assignments);
      }
    }
  }

  if (changed) {
    SPDLOG_DEBUG("Assignments for topic={} change to: {}", topic,
                 absl::StrJoin(assignments.begin(), assignments.end(), ",",
                               [](std::string* out, const rmq::Assignment& assignment) {
                                 out->append(assignment.DebugString());
                               }));
  }
}

thread_local std::size_t SimpleConsumerImpl::assignment_index_ = 0;

void SimpleConsumerImpl::refreshAssignment(const std::string& topic, std::function<void(const std::error_code&)> cb) {
  absl::flat_hash_set<std::string> endpoints;
  endpointsInUse(endpoints);
  if (endpoints.empty()) {
    SPDLOG_WARN("No broker is available");
    return;
  }

  rmq::QueryAssignmentRequest query_assignment_request;
  query_assignment_request.mutable_topic()->set_name(topic);
  query_assignment_request.mutable_topic()->set_resource_namespace(resourceNamespace());

  query_assignment_request.mutable_group()->CopyFrom(client_config_.subscriber.group);
  query_assignment_request.mutable_endpoints()->CopyFrom(accessPoint());

  Metadata metadata;
  Signature::sign(client_config_, metadata);

  std::weak_ptr<SimpleConsumerImpl> consumer(shared_from_this());
  auto callback = [consumer, topic, cb](const std::error_code& ec, const rmq::QueryAssignmentResponse& response) {
    auto simple_consumer = consumer.lock();
    const auto& assignments = response.assignments();
    if (assignments.empty()) {
      cb(ec);
      return;
    }

    std::vector<rmq::Assignment> assigns;
    assigns.insert(assigns.begin(), assignments.begin(), assignments.end());
    simple_consumer->updateAssignments(topic, assigns);
    cb(ec);
  };

  manager()->queryAssignment(*endpoints.begin(), metadata, query_assignment_request,
                             absl::ToChronoMilliseconds(client_config_.request_timeout), callback);
}

void SimpleConsumerImpl::refreshAssignments() {
  std::vector<std::string> topics;
  {
    absl::MutexLock lk(&subscriptions_mtx_);
    for (const auto& entry : subscriptions_) {
      topics.push_back(entry.first);
    }
  }

  auto mtx = std::make_shared<absl::Mutex>();
  auto cv = std::make_shared<absl::CondVar>();
  bool completed;
  for (const auto& topic : topics) {
    completed = false;
    auto callback = [&, mtx, cv](const std::error_code& ec) {
      absl::MutexLock lk(mtx.get());
      completed = true;
      cv->Signal();
    };

    refreshAssignment(topic, callback);

    {
      absl::MutexLock lk(mtx.get());
      if (!completed) {
        cv->Wait(mtx.get());
      }
    }
    SPDLOG_INFO("Assignments for {} received", topic);
  }
}

void SimpleConsumerImpl::receive(std::size_t limit,
                                 std::chrono::milliseconds invisible_duration,
                                 ReceiveCallback callback) {
  rmq::Assignment assignment;
  {
    absl::MutexLock lk(&assignments_mtx_);
    if (assignments_.empty()) {
      std::error_code ec = ErrorCode::NotFound;
      std::vector<MessageConstSharedPtr> messages;
      callback(ec, messages);
      return;
    }

    // choose assign allow readable
    std::size_t start_index = ++assignment_index_ % assignments_.size();
    for (std::size_t i = 0; i < assignments_.size(); ++i) {
      const auto& assign = assignments_[(start_index + i) % assignments_.size()];
      if (readable(assign.message_queue().permission())) {
        assignment.CopyFrom(assign);
        break;
      }
    }

    if (!assignment.IsInitialized()) {
      std::error_code ec = ErrorCode::NotFound;
      std::vector<MessageConstSharedPtr> messages;
      callback(ec, messages);
      return;
    }
  }

  const auto& target = urlOf(assignment.message_queue());
  Metadata metadata;
  Signature::sign(client_config_, metadata);

  rmq::ReceiveMessageRequest request;
  request.set_auto_renew(false);
  request.mutable_group()->CopyFrom(config().subscriber.group);
  request.mutable_message_queue()->CopyFrom(assignment.message_queue());
  request.set_batch_size((int32_t) limit);

  request.mutable_filter_expression()->set_type(rmq::FilterType::TAG);
  request.mutable_filter_expression()->set_expression("*");

  auto invisible_duration_request =
      google::protobuf::util::TimeUtil::MillisecondsToDuration(invisible_duration.count());
  request.mutable_invisible_duration()->set_nanos(invisible_duration_request.nanos());
  request.mutable_invisible_duration()->set_seconds(invisible_duration_request.seconds());

  auto await_duration_request =
      google::protobuf::util::TimeUtil::MillisecondsToDuration(
          MixAll::millisecondsOf(long_polling_duration_));
  request.mutable_long_polling_timeout()->set_nanos(await_duration_request.nanos());
  request.mutable_long_polling_timeout()->set_seconds(await_duration_request.seconds());

  auto cb = [callback](const std::error_code& ec, const ReceiveMessageResult& result) {
    std::vector<MessageConstSharedPtr> messages;
    if (ec) {
      callback(ec, messages);
      return;
    }

    callback(ec, result.messages);
  };

  manager()->receiveMessage(target, metadata, request,
                            long_polling_duration_ + absl::ToChronoMilliseconds(requestTimeout()), cb);
}

void SimpleConsumerImpl::wrapAckRequest(const Message& message, AckMessageRequest& request) {
  request.mutable_group()->CopyFrom(client_config_.subscriber.group);

  request.mutable_topic()->set_resource_namespace(resourceNamespace());
  request.mutable_topic()->set_name(message.topic());

  auto entry = new rmq::AckMessageEntry();
  entry->set_message_id(message.id());
  entry->set_receipt_handle(message.extension().receipt_handle);

  request.mutable_entries()->AddAllocated(entry);
}

void SimpleConsumerImpl::ack(const Message& message, std::error_code& ec) {
  Metadata metadata;
  Signature::sign(client_config_, metadata);
  AckMessageRequest request;
  wrapAckRequest(message, request);

  auto mtx = std::make_shared<absl::Mutex>();
  auto cv = std::make_shared<absl::CondVar>();
  bool completed = false;

  auto callback = [&, mtx, cv](const std::error_code& err) {
    absl::MutexLock lk(mtx.get());
    completed = true;
    ec = err;
    cv->Signal();
  };

  manager()->ack(message.extension().target_endpoint, metadata, request,
                 absl::ToChronoMilliseconds(client_config_.request_timeout), callback);

  {
    absl::MutexLock lk(mtx.get());
    if (!completed) {
      cv->Wait(mtx.get());
    }
  }
}

void SimpleConsumerImpl::ackAsync(const Message& message, AckCallback callback) {
  Metadata metadata;
  Signature::sign(client_config_, metadata);
  AckMessageRequest request;
  wrapAckRequest(message, request);
  manager()->ack(message.extension().target_endpoint, metadata, request,
                 absl::ToChronoMilliseconds(client_config_.request_timeout), callback);
}

void SimpleConsumerImpl::changeInvisibleDuration(const Message& message, std::string& receipt_handle,
                                                 std::chrono::milliseconds duration,
                                                 const ChangeInvisibleDurationCallback callback) {
  Metadata metadata;
  Signature::sign(client_config_, metadata);

  rmq::ChangeInvisibleDurationRequest request;
  request.mutable_group()->CopyFrom(client_config_.subscriber.group);
  request.mutable_topic()->set_resource_namespace(resourceNamespace());
  request.mutable_topic()->set_name(message.topic());
  request.set_message_id(message.id());
  request.set_receipt_handle(receipt_handle);
  auto d = google::protobuf::util::TimeUtil::MillisecondsToDuration(duration.count());
  request.mutable_invisible_duration()->CopyFrom(d);

  auto cb =
      [callback](const std::error_code& ec, const ChangeInvisibleDurationResponse& response) {
    std::string server_receipt_handle = response.receipt_handle();
    callback(ec, server_receipt_handle);
  };

  manager()->changeInvisibleDuration(
      message.extension().target_endpoint, metadata, request, duration, cb);
}

ROCKETMQ_NAMESPACE_END