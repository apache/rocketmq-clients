#pragma once

#include <cstddef>
#include <memory>
#include <vector>

#include "Configuration.h"
#include "Message.h"
#include "RocketMQ.h"
#include "SendCallback.h"

ROCKETMQ_NAMESPACE_BEGIN

class FifoProducerImpl;
class FifoProducerBuilder;
class ProducerImpl;

class FifoProducer {
public:
  static FifoProducerBuilder newBuilder();

  void send(MessageConstPtr message, SendCallback callback);

private:
  std::shared_ptr<FifoProducerImpl> impl_;

  explicit FifoProducer(std::shared_ptr<FifoProducerImpl> impl) : impl_(std::move(impl)) {
  }

  void start();

  friend class FifoProducerBuilder;
};

class FifoProducerBuilder {
public:
  FifoProducerBuilder();

  FifoProducerBuilder& withConfiguration(Configuration configuration);

  FifoProducerBuilder& withTopics(const std::vector<std::string>& topics);

  FifoProducerBuilder& withConcurrency(std::size_t concurrency);

  FifoProducer build();

private:
  std::shared_ptr<FifoProducerImpl> impl_;
  std::shared_ptr<ProducerImpl> producer_impl_;
};

ROCKETMQ_NAMESPACE_END