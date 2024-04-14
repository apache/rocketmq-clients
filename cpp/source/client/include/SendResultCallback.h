#pragma once

#include <functional>

#include "SendResult.h"

ROCKETMQ_NAMESPACE_BEGIN

using SendResultCallback = std::function<void(const SendResult&)>;

ROCKETMQ_NAMESPACE_END