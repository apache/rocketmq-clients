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

#include "opencensus/trace/exporter/link.h"

#include <cstdint>
#include <iostream>

#include "gtest/gtest.h"
#include "opencensus/trace/exporter/attribute_value.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_id.h"

namespace opencensus {
namespace trace {
namespace exporter {
namespace {

TEST(LinkTest, DebugStringIsNotEmpty) {
  const uint8_t trace_id[] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6};
  const uint8_t span_id[] = {11, 22, 33, 44, 55, 66, 77, 88};

  Link link(SpanContext(TraceId(trace_id), SpanId(span_id)),
            Link::Type::kParentLinkedSpan,
            {{"test", AttributeValue("attribute")}});
  std::cout << link.DebugString() << "\n";
  EXPECT_NE("", link.DebugString());
}

}  // namespace
}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
