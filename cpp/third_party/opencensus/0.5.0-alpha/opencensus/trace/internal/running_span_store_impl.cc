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

#include "opencensus/trace/internal/running_span_store_impl.h"

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/base/internal/endian.h"
#include "absl/base/thread_annotations.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "opencensus/trace/exporter/span_data.h"
#include "opencensus/trace/internal/span_impl.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"

namespace opencensus {
namespace trace {
namespace exporter {

namespace {
// Returns the memory address of the SpanImpl object, to be used as a key into
// the map of spans.
uintptr_t GetKey(const SpanImpl* span) {
  return reinterpret_cast<uintptr_t>(span);
}
}  // namespace

RunningSpanStoreImpl* RunningSpanStoreImpl::Get() {
  static RunningSpanStoreImpl* global_running_span_store =
      new RunningSpanStoreImpl;
  return global_running_span_store;
}

void RunningSpanStoreImpl::AddSpan(const std::shared_ptr<SpanImpl>& span) {
  absl::MutexLock l(&mu_);
  spans_.insert({GetKey(span.get()), span});
}

bool RunningSpanStoreImpl::RemoveSpan(const std::shared_ptr<SpanImpl>& span) {
  absl::MutexLock l(&mu_);
  auto iter = spans_.find(GetKey(span.get()));
  if (iter == spans_.end()) {
    return false;  // Not tracked.
  }
  spans_.erase(iter);
  return true;
}

RunningSpanStore::Summary RunningSpanStoreImpl::GetSummary() const {
  RunningSpanStore::Summary summary;
  absl::MutexLock l(&mu_);
  for (const auto& addr_span : spans_) {
    const std::string name = addr_span.second->name();
    auto it = summary.per_span_name_summary.find(name);
    if (it != summary.per_span_name_summary.end()) {
      it->second.num_running_spans++;
    } else {
      summary.per_span_name_summary[name] = {1};
    }
  }
  return summary;
}

std::vector<SpanData> RunningSpanStoreImpl::GetRunningSpans(
    const RunningSpanStore::Filter& filter) const {
  std::vector<SpanData> running_spans;
  absl::MutexLock l(&mu_);
  for (const auto& it : spans_) {
    if (running_spans.size() >= filter.max_spans_to_return) break;
    if (filter.span_name.empty() || (it.second->name() == filter.span_name)) {
      running_spans.emplace_back(it.second->ToSpanData());
    }
  }
  return running_spans;
}

void RunningSpanStoreImpl::ClearForTesting() {
  absl::MutexLock l(&mu_);
  spans_.clear();
}

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
