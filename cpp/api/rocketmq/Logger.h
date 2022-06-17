/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

#include "RocketMQ.h"

#ifndef SPDLOG_ACTIVE_LEVEL
#define SPDLOG_ACTIVE_LEVEL SPDLOG_LEVEL_TRACE
#endif

ROCKETMQ_NAMESPACE_BEGIN

enum class Level : uint8_t
{
  Trace = 0,
  Debug = 1,
  Info = 2,
  Warn = 3,
  Error = 4
};

class Logger {
public:
  virtual ~Logger() = default;

  /**
   * @brief Set log level for file sink. Its default log level is: Info.
   *
   * @param level
   */
  virtual void setLevel(Level level) = 0;

  /**
   * @brief Set log level for stdout, aka, console. Its default log level is: Warn.
   *
   * @param level
   */
  virtual void setConsoleLevel(Level level) = 0;

  virtual void setFileSize(std::size_t file_size) = 0;

  virtual void setFileCount(std::size_t file_count) = 0;

  virtual void setPattern(std::string pattern) = 0;

  virtual void init() = 0;
};

Logger& getLogger();

ROCKETMQ_NAMESPACE_END