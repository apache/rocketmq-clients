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
#include "LogInterceptor.h"

#include <cstddef>

#include "InterceptorContinuation.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_join.h"
#include "google/protobuf/message.h"
#include "rocketmq/Logger.h"
#include "spdlog/spdlog.h"

ROCKETMQ_NAMESPACE_BEGIN

void LogInterceptor::Intercept(grpc::experimental::InterceptorBatchMethods* methods) {
  InterceptorContinuation continuation(methods);

  auto level = spdlog::default_logger()->level();
  switch (level) {
    case spdlog::level::trace:
      // fall-through on purpose.
    case spdlog::level::debug: {
      break;
    }
    case spdlog::level::info:
      // fall-through on purpose.
    case spdlog::level::warn:
      // fall-through on purpose.
    case spdlog::level::err: {
      return;
    }
    default: {
      return;
    }
  }

  if (methods->QueryInterceptionHookPoint(grpc::experimental::InterceptionHookPoints::PRE_SEND_INITIAL_METADATA)) {
    std::multimap<std::string, std::string>* metadata = methods->GetSendInitialMetadata();
    if (metadata) {
      SPDLOG_DEBUG("[Outbound]Headers of {}: {}", client_rpc_info_->method(),
                   absl::StrJoin(*metadata, " ", absl::PairFormatter(" --> ")));
    }
  }

  if (methods->QueryInterceptionHookPoint(grpc::experimental::InterceptionHookPoints::PRE_SEND_MESSAGE)) {
    grpc::ByteBuffer* buffer = methods->GetSerializedSendMessage();
    if (buffer) {
      SPDLOG_DEBUG("[Outbound] {}: Buffer: {}bytes", client_rpc_info_->method(), buffer->Length());
    }
  }

  if (methods->QueryInterceptionHookPoint(grpc::experimental::InterceptionHookPoints::POST_RECV_INITIAL_METADATA)) {
    std::multimap<grpc::string_ref, grpc::string_ref>* metadata = methods->GetRecvInitialMetadata();
    if (metadata) {
      absl::flat_hash_map<absl::string_view, absl::string_view> response_headers;
      for (const auto& it : *metadata) {
        response_headers.insert({absl::string_view(it.first.data(), it.first.length()),
                                 absl::string_view(it.second.data(), it.second.length())});
      }
      if (!response_headers.empty()) {
        SPDLOG_DEBUG("[Inbound]Response Headers of {}: {}", client_rpc_info_->method(),
                     absl::StrJoin(response_headers, " ", absl::PairFormatter(" --> ")));
      } else {
        SPDLOG_DEBUG("[Inbound]Response metadata of {} is empty", client_rpc_info_->method());
      }
    }
  }

  if (methods->QueryInterceptionHookPoint(grpc::experimental::InterceptionHookPoints::POST_RECV_MESSAGE)) {
    void* message = methods->GetRecvMessage();
    if (message) {
      auto* response = reinterpret_cast<google::protobuf::Message*>(message);
      std::string&& response_text = response->ShortDebugString();
      std::size_t limit = 1024;
      if (response_text.size() <= limit) {
        SPDLOG_DEBUG("[Inbound] {} {}", client_rpc_info_->method(), response_text);
      } else {
        SPDLOG_DEBUG("[Inbound] {} {}...", client_rpc_info_->method(), response_text.substr(0, limit));
      }
    }
  }
}

ROCKETMQ_NAMESPACE_END