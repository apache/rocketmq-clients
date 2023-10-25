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
#include "StaticNameServerResolver.h"

#include "absl/strings/str_split.h"

#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

StaticNameServerResolver::StaticNameServerResolver(absl::string_view name_server_list) {
  // If the given endpoint already follows gRPC naming scheme, https://github.com/grpc/grpc/blob/master/doc/naming.md,
  // We should use it directly.
  if (naming_scheme_.accept(name_server_list)) {
    name_server_address_ = std::string(name_server_list.data(), name_server_list.size());
    return;
  }

  std::vector<std::string> segments = absl::StrSplit(name_server_list, absl::ByAnyChar(",;"));
  name_server_address_ = naming_scheme_.buildAddress(segments);
  if (name_server_address_.empty()) {
    SPDLOG_WARN("Failed to create gRPC naming scheme compliant address from {}",
                std::string(name_server_list.data(), name_server_list.length()));
  }
}

std::string StaticNameServerResolver::resolve() {
  return name_server_address_;
}

ROCKETMQ_NAMESPACE_END