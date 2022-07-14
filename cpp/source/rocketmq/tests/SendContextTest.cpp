#include <memory>

#include "SendContext.h"
#include "gtest/gtest.h"

ROCKETMQ_NAMESPACE_BEGIN

TEST(SendContextTest, testBasics) {
  auto message = Message::newBuilder().withBody("body").withTopic("topic").withGroup("group0").withTag("TagA").build();
  auto callback = [](const std::error_code&, const SendReceipt&) {};
  std::weak_ptr<ProducerImpl> producer;
  std::vector<rmq::MessageQueue> message_queues;
  auto send_context = std::make_shared<SendContext>(producer, std::move(message), callback, message_queues);
  send_context.reset();
}

ROCKETMQ_NAMESPACE_END