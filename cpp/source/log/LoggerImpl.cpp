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
#include "LoggerImpl.h"
#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <iostream>

// Check C++17 is used
#if defined(__cplusplus) && __cplusplus >= 201703L && defined(__has_include)
#if __has_include(<filesystem>)
#define GHC_USE_STD_FS
#include <filesystem>
namespace fs = std::filesystem;
#endif
#endif

#ifndef GHC_USE_STD_FS
#include "ghc/filesystem.hpp"
namespace fs = ghc::filesystem;
#endif

ROCKETMQ_NAMESPACE_BEGIN

const char* LoggerImpl::LOG_FILE = "/logs/rocketmq/client.log";
const char* LoggerImpl::LOGGER_NAME = "rocketmq_logger";

void LoggerImpl::setLevel(Level level) {
  level_ = level;
  auto logger = spdlog::get(LOGGER_NAME);
  if (logger) {
    setLevel(logger, level_);
  }
}

void LoggerImpl::setConsoleLevel(Level level) {
  console_level_ = level;
  setLevel(console_sink_, console_level_);
}

void LoggerImpl::init() {
  std::call_once(init_once_, [this] { init0(); });
}

void LoggerImpl::init0() {
  if (log_home_.empty()) {
    char* home = getenv(USER_HOME_ENV);
    if (home) {
      log_home_.append(home);
    } else {
      log_home_.append(fs::temp_directory_path().string());
    }
    log_home_.append(LOG_FILE);
  }

  // Create directories if necessary
  fs::path log_file(log_home_);
  fs::path log_dir = log_file.parent_path();
  if (!fs::exists(log_dir)) {
    if (!fs::create_directories(log_dir)) {
      std::cerr << "Failed to mkdir " << log_dir << std::endl;
      abort();
    }
  }
  // std::cout << "RocketMQ log files path: " << log_dir.c_str() << std::endl;

  if (pattern_.empty()) {
    pattern_ = DEFAULT_PATTERN;
  }

  console_sink_ = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
  setLevel(console_sink_, console_level_);
  console_sink_->set_pattern(pattern_);

  file_sink_ = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(log_home_, file_size_, file_count_,
                                                                      /*rotate_on_open=*/false);
  file_sink_->set_pattern(pattern_);
  setLevel(file_sink_, level_);

  // std::make_shared does not with initializer_list.
  auto default_logger =
      std::make_shared<spdlog::logger>(LOGGER_NAME, spdlog::sinks_init_list{console_sink_, file_sink_});
  default_logger->flush_on(spdlog::level::warn);
  Level logger_level =
      static_cast<Level>(std::min(static_cast<std::uint8_t>(level_), static_cast<std::uint8_t>(console_level_)));
  switch (logger_level) {
    case Level::Trace: {
      default_logger->set_level(spdlog::level::trace);
      break;
    }
    case Level::Debug: {
      default_logger->set_level(spdlog::level::debug);
      break;
    }
    case Level::Info: {
      default_logger->set_level(spdlog::level::info);
      break;
    }
    case Level::Warn: {
      default_logger->set_level(spdlog::level::warn);
      break;
    }
    default: {
      default_logger->set_level(spdlog::level::info);
      break;
    }
  }
  spdlog::flush_every(std::chrono::seconds(1));
  spdlog::set_default_logger(default_logger);
}

Logger& getLogger() {
  static LoggerImpl logger;
  return logger;
}

const std::size_t LoggerImpl::DEFAULT_MAX_LOG_FILE_QUANTITY = 16;
const std::size_t LoggerImpl::DEFAULT_FILE_SIZE = 1048576 * 256;
const char* LoggerImpl::USER_HOME_ENV = "HOME";
const char* LoggerImpl::DEFAULT_PATTERN = "%Y-%m-%d %H:%M:%S.%e [%^--%L--%$] [%7t] %v %@";
// const char* LoggerImpl::DEFAULT_PATTERN = "[%Y/%m/%d-%H:%M:%S.%e %z] [%n] [%^---%L---%$] [thread %t] %v %@";

ROCKETMQ_NAMESPACE_END