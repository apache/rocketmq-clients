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

#ifndef OPENCENSUS_TRACE_TRACE_ID_H_
#define OPENCENSUS_TRACE_TRACE_ID_H_

#include <cstdint>
#include <string>

namespace opencensus {
namespace trace {

// TraceId represents an opaque 128-bit trace identifier. The trace identifier
// remains constant across the trace. TraceId is immutable.
class TraceId final {
 public:
  // The size in bytes of the TraceId.
  static constexpr size_t kSize = 16;

  // An invalid TraceId (all zeros).
  TraceId() : rep_{0} {}

  // Creates a TraceId by copying the first kSize bytes from the buffer.
  explicit TraceId(const uint8_t* buf);

  // Returns a 32-char hex string of the TraceId value.
  std::string ToHex() const;

  // Returns a pointer to the opaque value.
  const void* Value() const;

  bool operator==(const TraceId& that) const;

  // Returns false if the TraceId is all zeros.
  bool IsValid() const;

  // Copies the opaque TraceId data to a buffer, which must hold kSize bytes.
  void CopyTo(uint8_t* buf) const;

 private:
  uint8_t rep_[kSize];
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_TRACE_ID_H_
