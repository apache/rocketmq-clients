#pragma once

#include "rocketmq/Message.h"
#include "rocketmq/RocketMQ.h"
#include "rocketmq/SendCallback.h"

ROCKETMQ_NAMESPACE_BEGIN

struct FifoContext {
  MessageConstPtr message;
  SendCallback callback;
};

ROCKETMQ_NAMESPACE_END