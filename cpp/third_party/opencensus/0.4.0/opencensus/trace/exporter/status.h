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

#ifndef OPENCENSUS_TRACE_EXPORTER_STATUS_H_
#define OPENCENSUS_TRACE_EXPORTER_STATUS_H_

#include <string>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "opencensus/trace/status_code.h"

namespace opencensus {
namespace trace {
namespace exporter {

// The Status of a Span is a StatusCode and a descriptive message if the code is
// not OK. Status is immutable.
class Status final {
 public:
  Status() : code_(StatusCode::OK) {}
  Status(StatusCode code, absl::string_view message)
      : code_(code), message_(code == StatusCode::OK ? "" : message) {}

  Status(const Status&) = default;
  Status(Status&&) = default;
  Status& operator=(const Status& x) = default;
  Status& operator=(Status&& x) = default;

  // Returns true if the Status is OK.
  ABSL_MUST_USE_RESULT bool ok() const { return code_ == StatusCode::OK; }

  // Returns the canonical code for this Status value.
  StatusCode CanonicalCode() const { return code_; }

  // Compares both code and message.
  bool operator==(const Status& that) const;
  bool operator!=(const Status& that) const;

  // Returns a combination of the error code name and message.
  std::string ToString() const;

  // Returns the error message. Note: prefer ToString() for debug logging.  This
  // message rarely describes the error code. It is not unusual for the error
  // message to be the empty string.
  const std::string& error_message() const { return message_; }

 private:
  StatusCode code_;
  std::string message_;
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_EXPORTER_STATUS_H_
