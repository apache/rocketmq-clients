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

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <memory>

#include "absl/base/thread_annotations.h"
#include "absl/memory/memory.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_split.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"

#include "NameServerResolver.h"
#include "NamingScheme.h"
#include "Scheduler.h"
#include "TopAddressing.h"

ROCKETMQ_NAMESPACE_BEGIN

class DynamicNameServerResolver : public NameServerResolver,
                                  public std::enable_shared_from_this<DynamicNameServerResolver> {
public:
  DynamicNameServerResolver(absl::string_view endpoint, std::chrono::milliseconds refresh_interval);

  void start() override;

  void shutdown() override;

  std::string resolve() override LOCKS_EXCLUDED(name_server_list_mtx_);

  void injectHttpClient(std::unique_ptr<HttpClient> http_client);

private:
  std::string endpoint_;

  SchedulerSharedPtr scheduler_;
  std::chrono::milliseconds refresh_interval_;

  void fetch();

  void onNameServerListFetched(const std::vector<std::string>& name_server_list) LOCKS_EXCLUDED(name_server_list_mtx_);

  std::vector<std::string> name_server_list_ GUARDED_BY(name_server_list_mtx_);
  absl::Mutex name_server_list_mtx_;

  std::atomic<std::uint32_t> index_{0};

  bool ssl_{false};
  std::unique_ptr<TopAddressing> top_addressing_;

  NamingScheme naming_scheme_;
};

ROCKETMQ_NAMESPACE_END