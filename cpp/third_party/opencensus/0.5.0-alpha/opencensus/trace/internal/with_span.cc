// Copyright 2018, OpenCensus Authors
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

#include "opencensus/trace/with_span.h"

#include <cassert>
#include <utility>

#include "opencensus/context/context.h"
#include "opencensus/trace/span.h"

using ::opencensus::context::Context;
using ::opencensus::trace::Span;

namespace opencensus {
namespace trace {

WithSpan::WithSpan(const Span& span, bool cond, bool end_span)
    : swapped_span_(span)
#ifndef NDEBUG
      ,
      original_context_(Context::InternalMutableCurrent())
#endif
      ,
      cond_(cond),
      end_span_(end_span) {
  ConditionalSwap();
}

WithSpan::~WithSpan() {
#ifndef NDEBUG
  assert(original_context_ == Context::InternalMutableCurrent() &&
         "WithSpan must be destructed on the same thread as it was "
         "constructed.");
#endif
  if (cond_ && end_span_) {
    Context::InternalMutableCurrent()->span_.End();
  }
  ConditionalSwap();
}

void WithSpan::ConditionalSwap() {
  if (cond_) {
    using std::swap;
    swap(Context::InternalMutableCurrent()->span_, swapped_span_);
  }
}

}  // namespace trace
}  // namespace opencensus
