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

#ifndef OPENCENSUS_TRACE_INTERNAL_TRACE_PARAMS_IMPL_H_
#define OPENCENSUS_TRACE_INTERNAL_TRACE_PARAMS_IMPL_H_

#include <atomic>
#include <cstdint>

#include "opencensus/trace/sampler.h"
#include "opencensus/trace/trace_params.h"

namespace opencensus {
namespace trace {

// TraceParamsImpl is used by TraceConfigImpl to hold the currently active
// TraceParams.
//
// TraceParamsImpl can be updated or retrieved via a TraceParams object.  Atomic
// reads or updates of multiple parameters are not supported, because reading
// from TraceParamsImpl happens on every StartSpan and we don't want to have a
// global lock.
class TraceParamsImpl final {
 public:
  explicit TraceParamsImpl(const TraceParams& p) { Set(p); }

  // TraceParamsImpl can be updated non-atomically: the
  // individual elements are updated separately.
  void Set(const TraceParams& p) {
    max_attributes_.store(p.max_attributes, std::memory_order_release);
    max_annotations_.store(p.max_annotations, std::memory_order_release);
    max_message_events_.store(p.max_message_events, std::memory_order_release);
    max_links_.store(p.max_links, std::memory_order_release);
    probability_threshold_.store(p.sampler.threshold_,
                                 std::memory_order_release);
  }

  TraceParams Get() const {
    return TraceParams{max_attributes_.load(std::memory_order_acquire),
                       max_annotations_.load(std::memory_order_acquire),
                       max_message_events_.load(std::memory_order_acquire),
                       max_links_.load(std::memory_order_acquire),
                       ProbabilitySampler(probability_threshold_.load(
                           std::memory_order_acquire))};
  }

 private:
  std::atomic<uint32_t> max_attributes_;
  std::atomic<uint32_t> max_annotations_;
  std::atomic<uint32_t> max_message_events_;
  std::atomic<uint32_t> max_links_;
  std::atomic<uint64_t> probability_threshold_;
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_INTERNAL_TRACE_PARAMS_IMPL_H_
