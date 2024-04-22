#include "rocketmq/FifoProducer.h"

#include <cstddef>
#include <memory>

#include "FifoProducerImpl.h"
#include "ProducerImpl.h"
#include "StaticNameServerResolver.h"
#include "rocketmq/Configuration.h"
#include "rocketmq/Message.h"
#include "rocketmq/RocketMQ.h"
#include "rocketmq/SendCallback.h"

ROCKETMQ_NAMESPACE_BEGIN

FifoProducerBuilder FifoProducer::newBuilder() {
  return {};
}

FifoProducerBuilder::FifoProducerBuilder() : producer_impl_(std::make_shared<ProducerImpl>()) {
}

FifoProducerBuilder& FifoProducerBuilder::withConfiguration(Configuration configuration) {
  auto name_server_resolver = std::make_shared<StaticNameServerResolver>(configuration.endpoints());
  producer_impl_->withNameServerResolver(std::move(name_server_resolver));
  producer_impl_->withCredentialsProvider(configuration.credentialsProvider());
  producer_impl_->withRequestTimeout(configuration.requestTimeout());
  producer_impl_->withSsl(configuration.withSsl());
  return *this;
}

FifoProducerBuilder& FifoProducerBuilder::withTopics(const std::vector<std::string>& topics) {
  producer_impl_->withTopics(topics);
  return *this;
}

FifoProducerBuilder& FifoProducerBuilder::withConcurrency(std::size_t concurrency) {
  this->impl_ = std::make_shared<FifoProducerImpl>(producer_impl_, concurrency);
  return *this;
}

FifoProducer FifoProducerBuilder::build() {
  FifoProducer fifo_producer(this->impl_);
  fifo_producer.start();
  return fifo_producer;
}

void FifoProducer::start() {
  impl_->internalProducer()->start();
}

void FifoProducer::send(MessageConstPtr message, SendCallback callback) {
  impl_->send(std::move(message), callback);
}

ROCKETMQ_NAMESPACE_END
