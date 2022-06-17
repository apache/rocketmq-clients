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
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <string>

#include "absl/memory/memory.h"
#include "absl/strings/match.h"
#include "fmt/format.h"
#include "ghc/filesystem.hpp"
#include "google/protobuf/struct.pb.h"
#include "google/protobuf/util/json_util.h"
#include "spdlog/spdlog.h"

#include "MixAll.h"
#include "StsCredentialsProviderImpl.h"
#include "rocketmq/Logger.h"

ROCKETMQ_NAMESPACE_BEGIN

StaticCredentialsProvider::StaticCredentialsProvider(std::string access_key, std::string access_secret)
    : access_key_(std::move(access_key)), access_secret_(std::move(access_secret)) {
}

Credentials StaticCredentialsProvider::getCredentials() {
  return Credentials(access_key_, access_secret_);
}

const char* EnvironmentVariablesCredentialsProvider::ENVIRONMENT_ACCESS_KEY = "ROCKETMQ_ACCESS_KEY";
const char* EnvironmentVariablesCredentialsProvider::ENVIRONMENT_ACCESS_SECRET = "ROCKETMQ_ACCESS_SECRET";

EnvironmentVariablesCredentialsProvider::EnvironmentVariablesCredentialsProvider() {
  char* key = getenv(ENVIRONMENT_ACCESS_KEY);
  if (key) {
    access_key_ = std::string(key);
  }

  char* secret = getenv(ENVIRONMENT_ACCESS_SECRET);
  if (secret) {
    access_secret_ = std::string(secret);
  }
}

Credentials EnvironmentVariablesCredentialsProvider::getCredentials() {
  return Credentials(access_key_, access_secret_);
}

const char* ConfigFileCredentialsProvider::CREDENTIAL_FILE_ = "rocketmq/credentials";

const char* ConfigFileCredentialsProvider::ACCESS_KEY_FIELD_NAME = "AccessKey";
const char* ConfigFileCredentialsProvider::ACCESS_SECRET_FIELD_NAME = "AccessSecret";

ConfigFileCredentialsProvider::ConfigFileCredentialsProvider() {
  std::string config_file;
  if (MixAll::homeDirectory(config_file)) {
    std::string path_separator(1, ghc::filesystem::path::preferred_separator);
    if (!absl::EndsWith(config_file, path_separator)) {
      config_file.append(1, ghc::filesystem::path::preferred_separator);
    }
    config_file.append(CREDENTIAL_FILE_);
    ghc::filesystem::path config_file_path(config_file);
    std::error_code ec;
    if (!ghc::filesystem::exists(config_file_path, ec) || ec) {
      SPDLOG_WARN("Config file[{}] does not exist.", config_file);
      return;
    }
    std::ifstream config_file_stream;
    config_file_stream.open(config_file.c_str(), std::ios::in | std::ios::ate | std::ios::binary);
    if (config_file_stream.good()) {
      auto size = config_file_stream.tellg();
      std::string content(size, '\0');
      config_file_stream.seekg(0);
      config_file_stream.read(&content[0], size);
      config_file_stream.close();
      google::protobuf::Struct root;
      google::protobuf::util::Status status = google::protobuf::util::JsonStringToMessage(content, &root);
      if (status.ok()) {
        auto&& fields = root.fields();
        if (fields.contains(ACCESS_KEY_FIELD_NAME)) {
          access_key_ = fields.at(ACCESS_KEY_FIELD_NAME).string_value();
        }

        if (fields.contains(ACCESS_SECRET_FIELD_NAME)) {
          access_secret_ = fields.at(ACCESS_SECRET_FIELD_NAME).string_value();
        }
        SPDLOG_DEBUG("Credentials for access_key={} loaded", access_key_);
      } else {
        SPDLOG_WARN("Failed to parse credential JSON config file. Message: {}", status.message().data());
      }
    } else {
      SPDLOG_WARN("Failed to open file: {}", config_file);
      return;
    }
  }
}

ConfigFileCredentialsProvider::ConfigFileCredentialsProvider(std::string config_file,
                                                             std::chrono::milliseconds refresh_interval) {
}

Credentials ConfigFileCredentialsProvider::getCredentials() {
  return Credentials(access_key_, access_secret_);
}

StsCredentialsProvider::StsCredentialsProvider(std::string ram_role_name)
    : impl_(absl::make_unique<StsCredentialsProviderImpl>(std::move(ram_role_name))) {
}

Credentials StsCredentialsProvider::getCredentials() {
  return impl_->getCredentials();
}

StsCredentialsProviderImpl::StsCredentialsProviderImpl(std::string ram_role_name)
    : ram_role_name_(std::move(ram_role_name)) {
}

StsCredentialsProviderImpl::~StsCredentialsProviderImpl() {
  http_client_->shutdown();
}

Credentials StsCredentialsProviderImpl::getCredentials() {
  if (std::chrono::system_clock::now() >= expiration_) {
    refresh();
  }

  {
    absl::MutexLock lk(&mtx_);
    return Credentials(access_key_, access_secret_, session_token_, expiration_);
  }
}

void StsCredentialsProviderImpl::refresh() {
  std::string path = fmt::format("{}{}", RAM_ROLE_URL_PREFIX, ram_role_name_);
  absl::Mutex sync_mtx;
  absl::CondVar sync_cv;
  bool completed = false;
  auto callback = [&, this](int code, const std::multimap<std::string, std::string>& headers, const std::string& body) {
    SPDLOG_DEBUG("Received STS response. Code: {}", code);
    if (static_cast<int>(HttpStatus::OK) == code) {
      google::protobuf::Struct doc;
      google::protobuf::util::Status status = google::protobuf::util::JsonStringToMessage(body, &doc);
      if (status.ok()) {
        const auto& fields = doc.fields();
        assert(fields.contains(FIELD_ACCESS_KEY));
        std::string access_key = fields.at(FIELD_ACCESS_KEY).string_value();
        assert(fields.contains(FIELD_ACCESS_SECRET));
        std::string access_secret = fields.at(FIELD_ACCESS_SECRET).string_value();
        assert(fields.contains(FIELD_SESSION_TOKEN));
        std::string session_token = fields.at(FIELD_SESSION_TOKEN).string_value();
        assert(fields.contains(FIELD_EXPIRATION));
        std::string expiration_string = fields.at(FIELD_EXPIRATION).string_value();
        absl::Time expiration_instant;
        std::string parse_error;
        if (absl::ParseTime(EXPIRATION_DATE_TIME_FORMAT, expiration_string, absl::UTCTimeZone(), &expiration_instant,
                            &parse_error)) {
          absl::MutexLock lk(&mtx_);
          access_key_ = std::move(access_key);
          access_secret_ = std::move(access_secret);
          session_token_ = std::move(session_token);
          expiration_ = absl::ToChronoTime(expiration_instant);
        } else {
          SPDLOG_WARN("Failed to parse expiration time. Message: {}", parse_error);
        }

      } else {
        SPDLOG_WARN("Failed to parse STS response. Message: {}", status.message().as_string());
      }
    } else {
      SPDLOG_WARN("STS response code is not OK. Code: {}", code);
    }

    {
      absl::MutexLock lk(&sync_mtx);
      completed = true;
      sync_cv.Signal();
    }
  };

  http_client_->get(HttpProtocol::HTTP, RAM_ROLE_HOST, 80, path, callback);

  while (!completed) {
    absl::MutexLock lk(&sync_mtx);
    sync_cv.Wait(&sync_mtx);
  }
}

const char* StsCredentialsProviderImpl::RAM_ROLE_HOST = "100.100.100.200";
const char* StsCredentialsProviderImpl::RAM_ROLE_URL_PREFIX = "/latest/meta-data/Ram/security-credentials/";
const char* StsCredentialsProviderImpl::FIELD_ACCESS_KEY = "AccessKeyId";
const char* StsCredentialsProviderImpl::FIELD_ACCESS_SECRET = "AccessKeySecret";
const char* StsCredentialsProviderImpl::FIELD_SESSION_TOKEN = "SecurityToken";
const char* StsCredentialsProviderImpl::FIELD_EXPIRATION = "Expiration";
const char* StsCredentialsProviderImpl::EXPIRATION_DATE_TIME_FORMAT = "%Y-%m-%d%ET%H:%H:%S%Ez";

ROCKETMQ_NAMESPACE_END