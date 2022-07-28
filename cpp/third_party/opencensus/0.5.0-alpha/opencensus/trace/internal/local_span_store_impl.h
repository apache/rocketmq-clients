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

#ifndef OPENCENSUS_TRACE_INTERNAL_LOCAL_SPAN_STORE_IMPL_H_
#define OPENCENSUS_TRACE_INTERNAL_LOCAL_SPAN_STORE_IMPL_H_

#include "opencensus/trace/internal/local_span_store.h"

#include <cstdint>
#include <deque>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/base/internal/endian.h"
#include "absl/base/thread_annotations.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "opencensus/trace/exporter/span_data.h"
#include "opencensus/trace/exporter/status.h"
#include "opencensus/trace/internal/span_impl.h"
#include "opencensus/trace/span.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"

namespace opencensus {
namespace trace {
namespace exporter {

// LocalSpanStoreImpl implements the LocalSpanStore API.
//
// This class is thread-safe and a singleton.
class LocalSpanStoreImpl {
 public:
  // Returns the global instance of LocalSpanStoreImpl.
  static LocalSpanStoreImpl* Get();

  // Adds a new running Span. Only Span::End should call this.
  void AddSpan(const std::shared_ptr<SpanImpl>& span) ABSL_LOCKS_EXCLUDED(mu_);

  // Returns a summary of the data available in the LocalSpanStore.
  LocalSpanStore::Summary GetSummary() const ABSL_LOCKS_EXCLUDED(mu_);

  // Returns the running spans that match the filter.
  std::vector<SpanData> GetLatencySampledSpans(
      const LocalSpanStore::LatencyFilter& filter) const
      ABSL_LOCKS_EXCLUDED(mu_);

  std::vector<SpanData> GetErrorSampledSpans(
      const LocalSpanStore::ErrorFilter& filter) const ABSL_LOCKS_EXCLUDED(mu_);

  std::vector<SpanData> GetSpans() const ABSL_LOCKS_EXCLUDED(mu_);

 private:
  friend class LocalSpanStoreImplTestPeer;

  // Private so only Get() can call it.
  LocalSpanStoreImpl() {}

  // Clears all currently active spans from the store.
  void ClearForTesting() ABSL_LOCKS_EXCLUDED(mu_);

  mutable absl::Mutex mu_;
  std::deque<SpanData> spans_ ABSL_GUARDED_BY(mu_);
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_INTERNAL_LOCAL_SPAN_STORE_IMPL_H_
