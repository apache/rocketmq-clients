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

#include "opencensus/trace/span_context.h"

#include "absl/strings/str_cat.h"

namespace opencensus {
namespace trace {

bool SpanContext::operator==(const SpanContext& that) const {
  return trace_id_ == that.trace_id() && span_id_ == that.span_id();
}

bool SpanContext::operator!=(const SpanContext& that) const {
  return !(*this == that);
}

bool SpanContext::IsValid() const {
  return trace_id_.IsValid() && span_id_.IsValid();
}

std::string SpanContext::ToString() const {
  return absl::StrCat(trace_id_.ToHex(), "-", span_id_.ToHex(), "-",
                      trace_options_.ToHex());
}

}  // namespace trace
}  // namespace opencensus
