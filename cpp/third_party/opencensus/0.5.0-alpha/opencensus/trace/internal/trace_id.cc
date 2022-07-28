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

#include "opencensus/trace/trace_id.h"

#include <cstring>
#include <string>

#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"

namespace opencensus {
namespace trace {

TraceId::TraceId(const uint8_t *buf) { memcpy(rep_, buf, kSize); }

std::string TraceId::ToHex() const {
  return absl::BytesToHexString(
      absl::string_view(reinterpret_cast<const char *>(rep_), kSize));
}

const void *TraceId::Value() const { return rep_; }

bool TraceId::operator==(const TraceId &that) const {
  return memcmp(rep_, that.rep_, kSize) == 0;
}

bool TraceId::IsValid() const {
  static_assert(kSize == 16, "Internal representation must be 16 bytes.");
  uint64_t tmp1;
  uint64_t tmp2;
  memcpy(&tmp1, &rep_[0], 8);
  memcpy(&tmp2, &rep_[8], 8);
  return (tmp1 != 0 || tmp2 != 0);
}

void TraceId::CopyTo(uint8_t *buf) const { memcpy(buf, rep_, kSize); }

}  // namespace trace
}  // namespace opencensus
