// Copyright 2018, OpenCensus Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "opencensus/common/internal/grpc/status.h"

#include <string>

#include <grpcpp/support/status.h>
#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"

namespace opencensus {
namespace common {

namespace {

absl::string_view StatusCodeName(grpc::StatusCode code) {
  switch (code) {
    case grpc::StatusCode::OK:
      return "OK";
    case grpc::StatusCode::CANCELLED:
      return "CANCELLED";
    case grpc::StatusCode::UNKNOWN:
      return "UNKNOWN";
    case grpc::StatusCode::INVALID_ARGUMENT:
      return "INVALID_ARGUMENT";
    case grpc::StatusCode::DEADLINE_EXCEEDED:
      return "DEADLINE_EXCEEDED";
    case grpc::StatusCode::NOT_FOUND:
      return "NOT_FOUND";
    case grpc::StatusCode::ALREADY_EXISTS:
      return "ALREADY_EXISTS";
    case grpc::StatusCode::PERMISSION_DENIED:
      return "PERMISSION_DENIED";
    case grpc::StatusCode::RESOURCE_EXHAUSTED:
      return "RESOURCE_EXHAUSTED";
    case grpc::StatusCode::FAILED_PRECONDITION:
      return "FAILED_PRECONDITION";
    case grpc::StatusCode::ABORTED:
      return "ABORTED";
    case grpc::StatusCode::OUT_OF_RANGE:
      return "OUT_OF_RANGE";
    case grpc::StatusCode::UNIMPLEMENTED:
      return "UNIMPLEMENTED";
    case grpc::StatusCode::INTERNAL:
      return "INTERNAL";
    case grpc::StatusCode::UNAVAILABLE:
      return "UNAVAILABLE";
    case grpc::StatusCode::DATA_LOSS:
      return "DATA_LOSS";
    case grpc::StatusCode::UNAUTHENTICATED:
      return "UNAUTHENTICATED";
    default:
      return "invalid status code value";
  }
}

}  // namespace

std::string ToString(const grpc::Status& status) {
  if (status.ok()) {
    return "OK";
  }
  return absl::StrCat(StatusCodeName(status.error_code()), ": ",
                      status.error_message());
}

}  // namespace common
}  // namespace opencensus
