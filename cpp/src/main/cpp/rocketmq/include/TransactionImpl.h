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
#include <string>

#include "rocketmq/Message.h"
#include "rocketmq/Transaction.h"

ROCKETMQ_NAMESPACE_BEGIN

class ProducerImpl;

class TransactionImpl : public Transaction {
public:
  TransactionImpl(std::string topic, std::string message_id, std::string trace_context, const std::weak_ptr<ProducerImpl>& producer)
      : topic_(std::move(topic)), message_id_(std::move(message_id)), trace_context_(std::move(trace_context)), producer_(producer) {
  }

  ~TransactionImpl() override = default;

  bool commit() override;

  bool rollback() override;

  const std::string& messageId() const override;

  const std::string& transactionId() const override;

  void transactionId(std::string transaction_id) {
    transaction_id_ = std::move(transaction_id);
  }

  const std::string& traceContext() const override {
    return trace_context_;
  }

  void traceContext(std::string trace_context) {
    trace_context_ = std::move(trace_context);
  }

  const std::string& endpoint() const override {
    return endpoint_;
  }

  void endpoint(std::string endpoint) { endpoint_ = std::move(endpoint); }

  const std::string& topic() const override {
    return topic_;
  }

private:
  std::string topic_;
  std::string message_id_;
  std::string transaction_id_;
  std::string endpoint_;
  std::string trace_context_;
  std::weak_ptr<ProducerImpl> producer_;
};

ROCKETMQ_NAMESPACE_END