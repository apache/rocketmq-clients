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

#include "opencensus/trace/span_id.h"

#include <cstring>
#include <string>

#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"

namespace opencensus {
namespace trace {

SpanId::SpanId(const uint8_t *buf) { memcpy(rep_, buf, kSize); }

std::string SpanId::ToHex() const {
  return absl::BytesToHexString(
      absl::string_view(reinterpret_cast<const char *>(rep_), kSize));
}

const void *SpanId::Value() const { return rep_; }

bool SpanId::operator==(const SpanId &that) const {
  return memcmp(rep_, that.rep_, kSize) == 0;
}

bool SpanId::IsValid() const {
  static_assert(kSize == 8,
                "IsValid assumes the internal representation is 8 bytes.");
  uint64_t tmp;
  memcpy(&tmp, rep_, kSize);
  return tmp != 0;
}

void SpanId::CopyTo(uint8_t *buf) const { memcpy(buf, rep_, kSize); }

}  // namespace trace
}  // namespace opencensus
