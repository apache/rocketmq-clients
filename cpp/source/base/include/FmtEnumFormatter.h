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

#include "fmt/format.h"
#include "rocketmq/State.h"
#include "apache/rocketmq/v2/definition.pb.h"
#include "grpcpp/support/status.h"

template <>
struct fmt::formatter<ROCKETMQ_NAMESPACE::State> : formatter<string_view> {
  template <typename FormatContext>
  auto format(ROCKETMQ_NAMESPACE::State s, FormatContext& ctx) -> decltype(ctx.out()) {
    string_view name;
    switch (s) {
      case ROCKETMQ_NAMESPACE::State::CREATED:  name = "CREATED"; break;
      case ROCKETMQ_NAMESPACE::State::STARTING: name = "STARTING"; break;
      case ROCKETMQ_NAMESPACE::State::STARTED:  name = "STARTED"; break;
      case ROCKETMQ_NAMESPACE::State::STOPPING: name = "STOPPING"; break;
      case ROCKETMQ_NAMESPACE::State::STOPPED:  name = "STOPPED"; break;
      default: name = "UNKNOWN"; break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};

template <>
struct fmt::formatter<grpc::StatusCode> : formatter<string_view> {
  template <typename FormatContext>
  auto format(grpc::StatusCode code, FormatContext& ctx) -> decltype(ctx.out()) {
    string_view name;
    switch (code) {
      case grpc::StatusCode::OK:                  name = "OK"; break;
      case grpc::StatusCode::CANCELLED:           name = "CANCELLED"; break;
      case grpc::StatusCode::UNKNOWN:             name = "UNKNOWN"; break;
      case grpc::StatusCode::INVALID_ARGUMENT:    name = "INVALID_ARGUMENT"; break;
      case grpc::StatusCode::DEADLINE_EXCEEDED:   name = "DEADLINE_EXCEEDED"; break;
      case grpc::StatusCode::NOT_FOUND:           name = "NOT_FOUND"; break;
      case grpc::StatusCode::ALREADY_EXISTS:      name = "ALREADY_EXISTS"; break;
      case grpc::StatusCode::PERMISSION_DENIED:   name = "PERMISSION_DENIED"; break;
      case grpc::StatusCode::UNAUTHENTICATED:     name = "UNAUTHENTICATED"; break;
      case grpc::StatusCode::RESOURCE_EXHAUSTED:  name = "RESOURCE_EXHAUSTED"; break;
      case grpc::StatusCode::FAILED_PRECONDITION: name = "FAILED_PRECONDITION"; break;
      case grpc::StatusCode::ABORTED:             name = "ABORTED"; break;
      case grpc::StatusCode::OUT_OF_RANGE:        name = "OUT_OF_RANGE"; break;
      case grpc::StatusCode::UNIMPLEMENTED:       name = "UNIMPLEMENTED"; break;
      case grpc::StatusCode::INTERNAL:            name = "INTERNAL"; break;
      case grpc::StatusCode::UNAVAILABLE:         name = "UNAVAILABLE"; break;
      case grpc::StatusCode::DATA_LOSS:           name = "DATA_LOSS"; break;
      default: name = "UNKNOWN"; break;
    }
    return formatter<string_view>::format(name, ctx);
  }
};

template <>
struct fmt::formatter<apache::rocketmq::v2::Code> : formatter<int> {
  template <typename FormatContext>
  auto format(apache::rocketmq::v2::Code code, FormatContext& ctx) -> decltype(ctx.out()) {
    return formatter<int>::format(static_cast<int>(code), ctx);
  }
};
