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
#include "rocketmq/Producer.h"

#include <memory>
#include <system_error>
#include <utility>

#include <spdlog/spdlog.h>

#include "ProducerImpl.h"
#include "StaticNameServerResolver.h"
#include "rocketmq/ErrorCode.h"
#include "rocketmq/SendReceipt.h"
#include "rocketmq/Transaction.h"
#include "rocketmq/RecallReceipt.h"

ROCKETMQ_NAMESPACE_BEGIN

void Producer::start() {
  impl_->start();
}

SendReceipt Producer::send(MessageConstPtr message, std::error_code& ec) noexcept {
  try {
    if (!message) {
      ec = ErrorCode::BadRequest;
      return {};
    }

    return impl_->send(std::move(message), ec);
  } catch (const std::exception& e) {
    ec = std::make_error_code(std::errc::io_error);
    SPDLOG_ERROR("Exception in send: {}", e.what());
    return {};
  }
}

void Producer::send(MessageConstPtr message, SendCallback callback) noexcept {
  try {
    if (!message) {
      std::error_code ec = ErrorCode::BadRequest;
      SendReceipt send_receipt = {};
      callback(ec, std::move(send_receipt));
      return;
    }

    if (!message->group().empty()) {
      SendReceipt     empty;
      std::error_code ec = ErrorCode::BadRequestAsyncPubFifoMessage;
      callback(ec, std::move(empty));
      return;
    }

    impl_->send(std::move(message), callback);
  } catch (const std::exception& e) {
    SPDLOG_ERROR("Exception in async send: {}", e.what());
    std::error_code ec = std::make_error_code(std::errc::io_error);
    SendReceipt empty;
    callback(ec, std::move(empty));
  }
}

std::unique_ptr<Transaction> Producer::beginTransaction() {
  return impl_->beginTransaction();
}

SendReceipt Producer::send(MessageConstPtr message, std::error_code& ec, Transaction& transaction) noexcept {
  try {
    return impl_->send(std::move(message), ec, transaction);
  } catch (const std::exception& e) {
    ec = std::make_error_code(std::errc::io_error);
    SPDLOG_ERROR("Exception in transactional send: {}", e.what());
    return {};
  }
}

RecallReceipt Producer::recall(std::string& topic, std::string& recall_handle, std::error_code& ec) noexcept {
  try {
    return impl_->recall(topic, recall_handle, ec);
  } catch (const std::exception& e) {
    ec = std::make_error_code(std::errc::io_error);
    SPDLOG_ERROR("Exception in recall: {}", e.what());
    return {};
  }
}

ProducerBuilder Producer::newBuilder() {
  return {};
}

ProducerBuilder::ProducerBuilder() : impl_(std::make_shared<ProducerImpl>()){}

ProducerBuilder& ProducerBuilder::withConfiguration(Configuration configuration) {
  auto name_server_resolver = std::make_shared<StaticNameServerResolver>(configuration.endpoints());
  impl_->withNameServerResolver(std::move(name_server_resolver));
  impl_->withResourceNamespace(configuration.resourceNamespace());
  impl_->withCredentialsProvider(configuration.credentialsProvider());
  impl_->withRequestTimeout(configuration.requestTimeout());
  impl_->withCallbackThreads(configuration.callbackThreads());
  impl_->withSsl(configuration.withSsl());
  return *this;
}

ProducerBuilder& ProducerBuilder::withTopics(std::vector<std::string> topics) {
  impl_->withTopics(std::move(topics));
  return *this;
}

ProducerBuilder& ProducerBuilder::withTransactionChecker(TransactionChecker checker) {
  impl_->transaction_checker_ = std::move(checker);
  return *this;
}

Producer ProducerBuilder::build() {
  Producer producer(impl_);
  producer.start();
  return producer;
}

ROCKETMQ_NAMESPACE_END