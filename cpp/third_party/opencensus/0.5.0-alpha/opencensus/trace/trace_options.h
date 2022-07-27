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

#ifndef OPENCENSUS_TRACE_TRACE_OPTIONS_H_
#define OPENCENSUS_TRACE_TRACE_OPTIONS_H_

#include <cstdint>
#include <string>

namespace opencensus {
namespace trace {

class SpanGenerator;
class SpanTestPeer;

// TraceOptions represents the options for a trace. These options are propagated
// to all child spans within the given trace. Currently only the IsSampled
// option is supported. TraceOptions is thread-compatible.
class TraceOptions final {
 public:
  // The size in bytes of the TraceOptions.
  static constexpr size_t kSize = 1;

  // No options are enabled by default.
  TraceOptions() : rep_{0} {}

  // Creates TraceOptions from a buffer of exactly kSize bytes.
  explicit TraceOptions(const uint8_t* buf);

  // IsSampled means that the trace should be sampled for exporting outside of
  // the process, e.g. to Stackdriver/Zipkin.
  bool IsSampled() const;

  bool operator==(const TraceOptions& that) const;

  // Returns a 2-char hex string of the TraceOptions value.
  std::string ToHex() const;

  // Copies the TraceOptions data to a buffer, which must hold kSize bytes.
  void CopyTo(uint8_t* buf) const;

  // Returns a copy of these TraceOptions with the sampled bit set to
  // is_sampled.
  TraceOptions WithSampling(bool is_sampled) const;

 private:
  uint8_t rep_[kSize];
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_TRACE_OPTIONS_H_
