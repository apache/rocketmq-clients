#include "FifoProducerPartition.h"

#include <absl/synchronization/mutex.h>

#include <atomic>
#include <memory>
#include <system_error>

#include "FifoContext.h"
#include "rocketmq/Message.h"
#include "rocketmq/RocketMQ.h"
#include "rocketmq/SendCallback.h"
#include "rocketmq/SendReceipt.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

void FifoProducerPartition::add(FifoContext&& context) {
  {
    absl::MutexLock lk(&messages_mtx_);
    messages_.emplace_back(std::move(context));
  }

  trySend();
}

void FifoProducerPartition::trySend() {
  bool expected = false;
  if (inflight_.compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
    absl::MutexLock lk(&messages_mtx_);

    FifoContext& ctx = messages_.front();
    MessageConstPtr message = std::move(ctx.message);
    SendCallback send_callback = ctx.callback;

    std::shared_ptr<FifoProducerPartition> partition = shared_from_this();
    auto fifo_callback = [=](const std::error_code& ec, const SendReceipt& receipt) mutable {
      partition->onComplete(ec, receipt, send_callback);
    };
    producer_->send(std::move(message), fifo_callback);
    messages_.pop_front();
  }
}

void FifoProducerPartition::onComplete(const std::error_code& ec, const SendReceipt& receipt, SendCallback& callback) {
  if (!ec) {
    callback(ec, receipt);
    // update inflight status
    bool expected = true;
    if (inflight_.compare_exchange_strong(expected, false, std::memory_order_relaxed)) {
      trySend();
    } else {
      SPDLOG_ERROR("Unexpected inflight status");
    }
    return;
  }

  // Put the message back to the front of the list
  SendReceipt& receipt_mut = const_cast<SendReceipt&>(receipt);
  FifoContext retry_context(std::move(receipt_mut.message), callback);
  {
    absl::MutexLock lk(&messages_mtx_);
    messages_.emplace_front(std::move(retry_context));
  }

  // Update inflight status
  bool expected = true;
  if (inflight_.compare_exchange_strong(expected, false, std::memory_order_relaxed)) {
    trySend();
  } else {
    SPDLOG_ERROR("Unexpected inflight status");
  }
}

ROCKETMQ_NAMESPACE_END