#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "FifoProducerPartition.h"
#include "ProducerImpl.h"
#include "rocketmq/Message.h"
#include "rocketmq/SendCallback.h"

ROCKETMQ_NAMESPACE_BEGIN

class FifoProducerImpl : std::enable_shared_from_this<FifoProducerImpl> {
public:
  FifoProducerImpl(std::shared_ptr<ProducerImpl> producer, std::size_t concurrency)
      : producer_(producer), concurrency_(concurrency), partitions_(concurrency) {
    for (auto i = 0; i < concurrency; i++) {
      partitions_[i] = std::make_shared<FifoProducerPartition>(producer_);
    }
  };

  void send(MessageConstPtr message, SendCallback callback);

private:
  std::shared_ptr<ProducerImpl> producer_;
  std::vector<std::shared_ptr<FifoProducerPartition>> partitions_;
  std::size_t concurrency_;
  std::hash<std::string> hash_fn_;
};

ROCKETMQ_NAMESPACE_END