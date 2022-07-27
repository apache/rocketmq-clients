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

#include "opencensus/trace/internal/local_span_store_impl.h"

#include <cstdint>
#include <memory>
#include <queue>
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

namespace {
constexpr int kMaxSpans = 128;

using ErrorFilter = LocalSpanStore::ErrorFilter;
using LatencyBucketBoundary = LocalSpanStore::LatencyBucketBoundary;
using LatencyFilter = LocalSpanStore::LatencyFilter;
using PerSpanNameSummary = LocalSpanStore::PerSpanNameSummary;
using Summary = LocalSpanStore::Summary;

// Returns a reference to the requested PerSpanNameSummary. If necessary, adds
// it first.
PerSpanNameSummary& GetPerSpanNameSummary(absl::string_view span_name,
                                          LocalSpanStore::Summary* summary) {
  const std::string name = std::string(span_name);
  return summary->per_span_name_summary
      .insert({name, LocalSpanStore::PerSpanNameSummary()})
      .first->second;
}

// Returns the LatencyBucketBoundary corresponding to the given latency.
LatencyBucketBoundary GetLatencyBucketBoundary(absl::Duration latency) {
  if (latency < absl::Microseconds(10))
    return LatencyBucketBoundary::k0_to_10us;
  if (latency < absl::Microseconds(100))
    return LatencyBucketBoundary::k10us_to_100us;
  if (latency < absl::Milliseconds(1))
    return LatencyBucketBoundary::k100us_to_1ms;
  if (latency < absl::Milliseconds(10))
    return LatencyBucketBoundary::k1ms_to_10ms;
  if (latency < absl::Milliseconds(100))
    return LatencyBucketBoundary::k10ms_to_100ms;
  if (latency < absl::Seconds(1)) return LatencyBucketBoundary::k100ms_to_1s;
  if (latency < absl::Seconds(10)) return LatencyBucketBoundary::k1s_to_10s;
  if (latency < absl::Seconds(100)) return LatencyBucketBoundary::k10s_to_100s;
  return LatencyBucketBoundary::k100s_plus;
}

// Increment the counter corresponding to the given key. Sets it to 1 if the key
// wasn't present before.
template <typename Key, typename Hash>
void MapIncrement(const Key& key, std::unordered_map<Key, int, Hash>* map) {
  auto iter_inserted = map->insert({key, 1});
  if (!iter_inserted.second) {
    // Was not inserted: needs to be incremented instead.
    ++iter_inserted.first->second;
  }
}

}  // namespace

LocalSpanStoreImpl* LocalSpanStoreImpl::Get() {
  static LocalSpanStoreImpl* global_running_span_store = new LocalSpanStoreImpl;
  return global_running_span_store;
}

void LocalSpanStoreImpl::AddSpan(const std::shared_ptr<SpanImpl>& span) {
  absl::MutexLock l(&mu_);
  if (spans_.size() >= kMaxSpans) {
    spans_.pop_back();  // Make room.
  }
  spans_.emplace_front(span->ToSpanData());
}

Summary LocalSpanStoreImpl::GetSummary() const {
  Summary summary;
  absl::MutexLock l(&mu_);
  for (const auto& span : spans_) {
    PerSpanNameSummary& curr = GetPerSpanNameSummary(span.name(), &summary);
    const absl::Duration latency = span.end_time() - span.start_time();
    MapIncrement(GetLatencyBucketBoundary(latency),
                 &curr.number_of_latency_sampled_spans);
    MapIncrement(span.status().CanonicalCode(),
                 &curr.number_of_error_sampled_spans);
  }
  return summary;
}

std::vector<SpanData> LocalSpanStoreImpl::GetLatencySampledSpans(
    const LatencyFilter& filter) const {
  std::vector<SpanData> out;
  absl::MutexLock l(&mu_);
  for (const auto& span : spans_) {
    uint64_t latency_ns =
        (span.end_time() - span.start_time()) / absl::Nanoseconds(1);
    if ((filter.span_name.empty() || (span.name() == filter.span_name)) &&
        latency_ns >= filter.lower_latency_ns &&
        latency_ns < filter.upper_latency_ns) {
      out.emplace_back(span);
    }
    if (out.size() >= filter.max_spans_to_return) break;
  }
  return out;
}

std::vector<SpanData> LocalSpanStoreImpl::GetErrorSampledSpans(
    const ErrorFilter& filter) const {
  std::vector<SpanData> out;
  absl::MutexLock l(&mu_);
  for (const auto& span : spans_) {
    if ((filter.span_name.empty() || (span.name() == filter.span_name)) &&
        filter.canonical_code == span.status().CanonicalCode()) {
      out.emplace_back(span);
    }
    if (out.size() >= filter.max_spans_to_return) break;
  }
  return out;
}

std::vector<SpanData> LocalSpanStoreImpl::GetSpans() const {
  absl::MutexLock l(&mu_);
  std::vector<SpanData> out(spans_.begin(), spans_.end());
  return out;
}

void LocalSpanStoreImpl::ClearForTesting() {
  absl::MutexLock l(&mu_);
  spans_.clear();
}

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
