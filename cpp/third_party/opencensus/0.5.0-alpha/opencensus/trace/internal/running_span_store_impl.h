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

#ifndef OPENCENSUS_TRACE_INTERNAL_RUNNING_SPAN_STORE_IMPL_H_
#define OPENCENSUS_TRACE_INTERNAL_RUNNING_SPAN_STORE_IMPL_H_

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

#include "absl/base/thread_annotations.h"
#include "absl/synchronization/mutex.h"
#include "opencensus/trace/internal/running_span_store.h"
#include "opencensus/trace/internal/span_impl.h"

namespace opencensus {
namespace trace {
namespace exporter {

// RunningSpanStoreImpl implements the store for the RunningSpanStore API.
//
// This class is thread-safe and a singleton.
class RunningSpanStoreImpl {
 public:
  // Returns the global instance of RunningSpanStoreImpl.
  static RunningSpanStoreImpl* Get();

  // Adds a new running Span.
  void AddSpan(const std::shared_ptr<SpanImpl>& span) ABSL_LOCKS_EXCLUDED(mu_);

  // Removes a Span that's no longer running. Returns true on success, false if
  // that Span was not being tracked.
  bool RemoveSpan(const std::shared_ptr<SpanImpl>& span)
      ABSL_LOCKS_EXCLUDED(mu_);

  // Returns a summary of the data available in the RunningSpanStore.
  RunningSpanStore::Summary GetSummary() const ABSL_LOCKS_EXCLUDED(mu_);

  // Returns the running spans that match the filter.
  std::vector<SpanData> GetRunningSpans(
      const RunningSpanStore::Filter& filter) const ABSL_LOCKS_EXCLUDED(mu_);

 private:
  friend class RunningSpanStoreImplTestPeer;

  RunningSpanStoreImpl() {}

  // Clears all currently active spans from the store.
  void ClearForTesting() ABSL_LOCKS_EXCLUDED(mu_);

  mutable absl::Mutex mu_;

  // The key is the memory address of the underlying SpanImpl object.
  std::unordered_map<uintptr_t, std::shared_ptr<SpanImpl>> spans_
      ABSL_GUARDED_BY(mu_);
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_INTERNAL_RUNNING_SPAN_STORE_IMPL_H_
