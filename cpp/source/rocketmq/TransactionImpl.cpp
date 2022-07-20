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
#include "TransactionImpl.h"
#include "ProducerImpl.h"
#include <string>

ROCKETMQ_NAMESPACE_BEGIN

bool TransactionImpl::commit() {
  std::shared_ptr<ProducerImpl> producer = producer_.lock();
  if (!producer) {
    return false;
  }

  bool result = true;
  {
    absl::MutexLock lk(&pending_transactions_mtx_);
    for (const auto& mini : pending_transactions_) {
      result &= producer->endTransaction0(mini, TransactionState::COMMIT);
    }
  }
  return result;
}

bool TransactionImpl::rollback() {
  std::shared_ptr<ProducerImpl> producer = producer_.lock();
  if (!producer) {
    return false;
  }

  bool result = true;
  {
    absl::MutexLock lk(&pending_transactions_mtx_);
    for (const auto& mini : pending_transactions_) {
      result &= producer->endTransaction0(mini, TransactionState::ROLLBACK);
    }
  }
  return result;
}

ROCKETMQ_NAMESPACE_END