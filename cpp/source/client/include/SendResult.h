#pragma once

#include <system_error>

#include "rocketmq/RocketMQ.h"

ROCKETMQ_NAMESPACE_BEGIN

struct SendResult {
  std::error_code ec;
  std::string target;

  std::string message_id;
  std::string transaction_id;
};

ROCKETMQ_NAMESPACE_END