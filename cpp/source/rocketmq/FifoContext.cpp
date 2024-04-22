#include "FifoContext.h"

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

FifoContext::FifoContext(MessageConstPtr message, SendCallback callback)
    : message(std::move(message)), callback(callback) {
}

FifoContext::FifoContext(FifoContext&& rhs) noexcept {
  this->message = std::move(rhs.message);
  this->callback = rhs.callback;
}

ROCKETMQ_NAMESPACE_END