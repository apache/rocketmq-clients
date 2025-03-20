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
#include <system_error>
#include <vector>

#include "Configuration.h"
#include "Message.h"
#include "RecallReceipt.h"
#include "SendCallback.h"
#include "SendReceipt.h"
#include "Transaction.h"
#include "TransactionChecker.h"
#include "rocketmq/Logger.h"

ROCKETMQ_NAMESPACE_BEGIN

/**
 * This class employs pointer-to-implementation paradigm to achieve the goal of stable ABI.
 * Refer https://en.cppreference.com/w/cpp/language/pimpl for an explanation.
 */
class ProducerImpl;

class ProducerBuilder;

/**
 * @brief
 *
 */
class Producer {
public:
  static ProducerBuilder newBuilder();

  /**
   * @brief Send message synchronously.
   *
   * @param message Message to publish. Note the pointer, std::unique_ptr<const Message>, is 'moved' into.
   * @param ec Error code and message
   * @return SendReceipt Receipt of the pub action if successful, which holds an identifier for the message.
   */
  SendReceipt send(MessageConstPtr message, std::error_code& ec) noexcept;

  /**
   * @brief Send message in asynchronous manner.
   *
   * @param message  Message to send. Note the pointer, std::unique_ptr<const Message>, is 'moved' into.
   * @param callback Callback to execute on completion.
   */
  void send(MessageConstPtr message, const SendCallback& callback) noexcept;

  std::unique_ptr<Transaction> beginTransaction();

  SendReceipt send(MessageConstPtr message, std::error_code& ec, Transaction& transaction);

  /**
   * @brief Attempts to cancel a scheduled message based on the provided topic and recall handle.
   * This operation requires server support to be executed successfully.
   *
   * @param topic The topic associated with the scheduled message to be canceled.
   * @param recall_handle A unique identifier or handle for the message cancellation operation.
   * @param ec An error code that will be set if the operation encounters an error.
   * @return RecallReceipt A receipt object indicating the result of the cancellation operation.
   */
  RecallReceipt recall(std::string& topic, std::string& recall_handle, std::error_code& ec) noexcept;

private:
  explicit Producer(std::shared_ptr<ProducerImpl> impl) : impl_(std::move(impl)) {
  }

  void start();

  std::shared_ptr<ProducerImpl> impl_;

  friend class ProducerBuilder;
};

class ProducerBuilder {
public:
  ProducerBuilder();

  ProducerBuilder& withConfiguration(Configuration configuration);

  ProducerBuilder& withTopics(const std::vector<std::string>& topics);

  ProducerBuilder& withTransactionChecker(const TransactionChecker& checker);

  Producer build();

private:
  std::shared_ptr<ProducerImpl> impl_;
};

ROCKETMQ_NAMESPACE_END