#include "FifoProducerImpl.h"

#include <utility>

#include "FifoContext.h"
#include "rocketmq/Message.h"
#include "rocketmq/RocketMQ.h"
#include "rocketmq/SendCallback.h"

ROCKETMQ_NAMESPACE_BEGIN

void FifoProducerImpl::send(MessageConstPtr message, SendCallback callback) {
  auto& group = message->group();
  std::size_t hash = hash_fn_(group);
  std::size_t slot = hash % concurrency_;

  FifoContext context(std::move(message), callback);
  partitions_[slot]->add(std::move(context));
}

ROCKETMQ_NAMESPACE_END