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

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "rocketmq/Message.h"
#include "rocketmq/Transaction.h"

ROCKETMQ_NAMESPACE_BEGIN

class ProducerImpl;

struct MiniTransaction {
  std::string topic;
  std::string message_id;
  std::string transaction_id;
  std::string trace_context;
  std::string target;
};

class TransactionImpl : public Transaction {
public:
  TransactionImpl(const std::weak_ptr<ProducerImpl>& producer) : producer_(producer) {
  }

  ~TransactionImpl() override = default;

  bool commit() override;

  bool rollback() override;

  void appendMiniTransaction(MiniTransaction mini_transaction) LOCKS_EXCLUDED(pending_transactions_mtx_) {
    absl::MutexLock lk(&pending_transactions_mtx_);
    pending_transactions_.emplace_back(std::move(mini_transaction));
  }

private:
  std::weak_ptr<ProducerImpl> producer_;
  std::vector<MiniTransaction> pending_transactions_ GUARDED_BY(pending_transactions_mtx_);
  absl::Mutex pending_transactions_mtx_;
};

ROCKETMQ_NAMESPACE_END