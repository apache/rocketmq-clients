// Copyright 2017, OpenCensus Authors
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

#include "opencensus/trace/exporter/status.h"

#include <string>

#include "absl/strings/str_cat.h"

namespace opencensus {
namespace trace {
namespace exporter {
namespace {

absl::string_view CodeToString(StatusCode code) {
  switch (code) {
    case StatusCode::OK:
      return "OK";
    case StatusCode::CANCELLED:
      return "CANCELLED";
    case StatusCode::UNKNOWN:
      return "UNKNOWN";
    case StatusCode::INVALID_ARGUMENT:
      return "INVALID_ARGUMENT";
    case StatusCode::DEADLINE_EXCEEDED:
      return "DEADLINE_EXCEEDED";
    case StatusCode::NOT_FOUND:
      return "NOT_FOUND";
    case StatusCode::ALREADY_EXISTS:
      return "ALREADY_EXISTS";
    case StatusCode::PERMISSION_DENIED:
      return "PERMISSION_DENIED";
    case StatusCode::RESOURCE_EXHAUSTED:
      return "RESOURCE_EXHAUSTED";
    case StatusCode::FAILED_PRECONDITION:
      return "FAILED_PRECONDITION";
    case StatusCode::ABORTED:
      return "ABORTED";
    case StatusCode::OUT_OF_RANGE:
      return "OUT_OF_RANGE";
    case StatusCode::UNIMPLEMENTED:
      return "UNIMPLEMENTED";
    case StatusCode::INTERNAL:
      return "INTERNAL";
    case StatusCode::UNAVAILABLE:
      return "UNAVAILABLE";
    case StatusCode::DATA_LOSS:
      return "DATA_LOSS";
    case StatusCode::UNAUTHENTICATED:
      return "UNAUTHENTICATED";
  }
  return "";
}

}  // namespace

std::string Status::ToString() const {
  if (ok()) {
    return "OK";
  }
  return absl::StrCat(CodeToString(code_), ": ", message_);
}

bool Status::operator==(const Status& that) const {
  return code_ == that.code_ && message_ == that.message_;
}

bool Status::operator!=(const Status& that) const { return !(*this == that); }

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
