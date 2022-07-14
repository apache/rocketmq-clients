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
#include "rocketmq/Logger.h"
#include "spdlog/sinks/rotating_file_sink.h"
#include "spdlog/sinks/stdout_color_sinks.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

class LoggerImpl : public Logger {

public:
  ~LoggerImpl() override = default;

  void setLevel(Level level) override;

  void setConsoleLevel(Level level) override;

  void setFileSize(std::size_t file_size) override {
    file_size_ = file_size;
  }

  void setFileCount(std::size_t file_count) override {
    file_count_ = file_count;
  }

  void setPattern(std::string pattern) override {
    pattern_ = std::move(pattern);
    auto logger = spdlog::get(LOGGER_NAME);
    if (logger) {
      logger->set_pattern(pattern_);
    }
  }

  void init() override;

  static const char* LOGGER_NAME;

private:
  void init0();

  template <typename T>
  void setLevel(std::shared_ptr<T>& target, Level level) {
    if (!target) {
      return;
    }

    switch (level) {
      case Level::Trace:
        target->set_level(spdlog::level::trace);
        break;
      case Level::Debug:
        target->set_level(spdlog::level::debug);
        break;
      case Level::Info:
        target->set_level(spdlog::level::info);
        break;
      case Level::Warn:
        target->set_level(spdlog::level::warn);
        break;
      case Level::Error:
        target->set_level(spdlog::level::err);
        break;
    }
  }

  static const char* LOG_FILE;

  Level level_{Level::Info};
  Level console_level_{Level::Warn};
  std::string log_home_;
  std::size_t file_size_{DEFAULT_FILE_SIZE};
  std::size_t file_count_{DEFAULT_MAX_LOG_FILE_QUANTITY};
  std::string pattern_;

  std::once_flag init_once_;

  std::shared_ptr<spdlog::sinks::rotating_file_sink_mt> file_sink_;
  std::shared_ptr<spdlog::sinks::stdout_color_sink_mt> console_sink_;

  static const std::size_t DEFAULT_MAX_LOG_FILE_QUANTITY;
  static const std::size_t DEFAULT_FILE_SIZE;
  static const char* DEFAULT_PATTERN;
  static const char* USER_HOME_ENV;
};

ROCKETMQ_NAMESPACE_END