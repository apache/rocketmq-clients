#pragma once

#include <absl/base/internal/thread_annotations.h>

#include <atomic>
#include <list>
#include <memory>
#include <system_error>

#include "FifoContext.h"
#include "ProducerImpl.h"
#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "rocketmq/SendCallback.h"
#include "rocketmq/SendReceipt.h"

ROCKETMQ_NAMESPACE_BEGIN

class FifoProducerPartition : public std::enable_shared_from_this<FifoProducerPartition> {
public:
  FifoProducerPartition(std::shared_ptr<ProducerImpl> producer, std::string&& name)
      : producer_(producer), name_(std::move(name)) {
  }

  void add(FifoContext&& context) LOCKS_EXCLUDED(messages_mtx_);

  void trySend() LOCKS_EXCLUDED(messages_mtx_);

  void onComplete(const std::error_code& ec, const SendReceipt& receipt, SendCallback& callback);

private:
  std::shared_ptr<ProducerImpl> producer_;
  std::list<FifoContext> messages_ GUARDED_BY(messages_mtx_);
  absl::Mutex messages_mtx_;
  std::atomic_bool inflight_{false};
  std::string name_;
};

ROCKETMQ_NAMESPACE_END