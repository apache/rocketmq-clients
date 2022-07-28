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

#include "opencensus/trace/internal/local_span_store.h"

#include "gtest/gtest.h"
#include "opencensus/trace/internal/local_span_store_impl.h"
#include "opencensus/trace/sampler.h"
#include "opencensus/trace/span.h"

namespace opencensus {
namespace trace {

namespace exporter {
class LocalSpanStoreImplTestPeer {
 public:
  static void ClearForTesting() {
    LocalSpanStoreImpl::Get()->ClearForTesting();
  }
};

namespace {

TEST(LocalSpanStoreTest, GetSummary) {
  exporter::LocalSpanStoreImplTestPeer::ClearForTesting();
  static AlwaysSampler sampler;
  auto span = Span::StartSpan("SpanName", /*parent=*/nullptr, {&sampler});
  span.AddAnnotation("Annotation");
  span.End();

  auto summary = LocalSpanStore::GetSummary();
  EXPECT_EQ(1, summary.per_span_name_summary.size());
  EXPECT_EQ(1, summary.per_span_name_summary["SpanName"]
                   .number_of_latency_sampled_spans.size());
  // TODO: EXPECT it ended up in the right bucket in
  // number_of_latency_sampled_spans[].
}

}  // namespace
}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
