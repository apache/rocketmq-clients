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

#include "opencensus/trace/trace_options.h"

#include <cstdint>
#include <string>

#include "absl/strings/escaping.h"
#include "absl/strings/string_view.h"

namespace opencensus {
namespace trace {
namespace {
// One bit per option.
constexpr uint8_t kIsSampled = 1;
}  // namespace

TraceOptions::TraceOptions(const uint8_t* buf) { memcpy(rep_, buf, kSize); }

bool TraceOptions::IsSampled() const { return rep_[0] & kIsSampled; }

bool TraceOptions::operator==(const TraceOptions& that) const {
  return memcmp(rep_, that.rep_, kSize) == 0;
}

std::string TraceOptions::ToHex() const {
  return absl::BytesToHexString(
      absl::string_view(reinterpret_cast<const char*>(rep_), kSize));
}

void TraceOptions::CopyTo(uint8_t* buf) const { memcpy(buf, rep_, kSize); }

TraceOptions TraceOptions::WithSampling(bool is_sampled) const {
  uint8_t buf[kSize];
  CopyTo(buf);
  buf[0] = (buf[0] & ~kIsSampled) | (is_sampled ? kIsSampled : 0);
  return TraceOptions(buf);
}

}  // namespace trace
}  // namespace opencensus
