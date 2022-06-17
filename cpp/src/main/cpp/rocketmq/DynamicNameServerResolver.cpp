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
#include "DynamicNameServerResolver.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>

#include "absl/strings/str_join.h"

#include "LoggerImpl.h"
#include "SchedulerImpl.h"

ROCKETMQ_NAMESPACE_BEGIN

DynamicNameServerResolver::DynamicNameServerResolver(absl::string_view endpoint,
                                                     std::chrono::milliseconds refresh_interval)
    : endpoint_(endpoint.data(), endpoint.length()), scheduler_(std::make_shared<SchedulerImpl>(1)),
      refresh_interval_(refresh_interval) {
  absl::string_view remains;
  if (absl::StartsWith(endpoint_, "https://")) {
    ssl_ = true;
    remains = absl::StripPrefix(endpoint_, "https://");
  } else {
    remains = absl::StripPrefix(endpoint_, "http://");
  }

  std::int32_t port = 80;
  if (ssl_) {
    port = 443;
  }

  absl::string_view host;
  if (absl::StrContains(remains, ':')) {
    std::vector<absl::string_view> segments = absl::StrSplit(remains, ':');
    host = segments[0];
    remains = absl::StripPrefix(remains, host);
    remains = absl::StripPrefix(remains, ":");

    segments = absl::StrSplit(remains, '/');
    if (!absl::SimpleAtoi(segments[0], &port)) {
      SPDLOG_WARN("Failed to parse port of name-server-list discovery service endpoint");
      abort();
    }
    remains = absl::StripPrefix(remains, segments[0]);
  } else {
    std::vector<absl::string_view> segments = absl::StrSplit(remains, '/');
    host = segments[0];
    remains = absl::StripPrefix(remains, host);
  }

  top_addressing_ = absl::make_unique<TopAddressing>(std::string(host.data(), host.length()), port,
                                                     std::string(remains.data(), remains.length()));
}

std::string DynamicNameServerResolver::resolve() {
  bool fetch_immediately = false;
  {
    absl::MutexLock lk(&name_server_list_mtx_);
    if (name_server_list_.empty()) {
      fetch_immediately = true;
    }
  }

  if (fetch_immediately) {
    fetch();
  }

  {
    absl::MutexLock lk(&name_server_list_mtx_);
    return naming_scheme_.buildAddress(name_server_list_);
  }
}

void DynamicNameServerResolver::fetch() {
  std::weak_ptr<DynamicNameServerResolver> ptr(shared_from_this());
  auto callback = [ptr](bool success, const std::vector<std::string>& name_server_list) {
    if (success && !name_server_list.empty()) {
      std::shared_ptr<DynamicNameServerResolver> resolver = ptr.lock();
      if (resolver) {
        resolver->onNameServerListFetched(name_server_list);
      }
    }
  };
  top_addressing_->fetchNameServerAddresses(callback);
}

void DynamicNameServerResolver::onNameServerListFetched(const std::vector<std::string>& name_server_list) {
  if (!name_server_list.empty()) {
    absl::MutexLock lk(&name_server_list_mtx_);
    if (name_server_list_ != name_server_list) {
      SPDLOG_INFO("Name server list changed. {} --> {}", absl::StrJoin(name_server_list_, ";"),
                  absl::StrJoin(name_server_list, ";"));
      name_server_list_ = name_server_list;
    }
  }
}

void DynamicNameServerResolver::injectHttpClient(std::unique_ptr<HttpClient> http_client) {
  top_addressing_->injectHttpClient(std::move(http_client));
}

void DynamicNameServerResolver::start() {
  scheduler_->start();
  scheduler_->schedule(std::bind(&DynamicNameServerResolver::fetch, this), "DynamicNameServerResolver",
                       std::chrono::milliseconds(0), refresh_interval_);
}

void DynamicNameServerResolver::shutdown() {
  scheduler_->shutdown();
}

ROCKETMQ_NAMESPACE_END