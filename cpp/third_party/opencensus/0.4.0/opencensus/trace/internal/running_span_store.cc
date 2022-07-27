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

#include "opencensus/trace/internal/running_span_store.h"

#include <vector>

#include "opencensus/trace/exporter/span_data.h"
#include "opencensus/trace/internal/running_span_store_impl.h"

namespace opencensus {
namespace trace {
namespace exporter {

RunningSpanStore::Summary RunningSpanStore::GetSummary() {
  return RunningSpanStoreImpl::Get()->GetSummary();
}

std::vector<SpanData> RunningSpanStore::GetRunningSpans(const Filter& filter) {
  return RunningSpanStoreImpl::Get()->GetRunningSpans(filter);
}

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
