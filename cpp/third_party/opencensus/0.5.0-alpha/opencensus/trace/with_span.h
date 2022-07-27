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

#ifndef OPENCENSUS_TRACE_WITH_SPAN_H_
#define OPENCENSUS_TRACE_WITH_SPAN_H_

#include "opencensus/context/context.h"
#include "opencensus/trace/span.h"

namespace opencensus {
namespace trace {

// WithSpan is a scoped object that sets the current Span to the given one,
// until the WithSpan object is destroyed. If the condition is false, it doesn't
// do anything. If the condition is true and end_span is true, it calls End() on
// the Span when it falls out of scope.
//
// Because WithSpan changes the current (thread local) context, NEVER allocate a
// WithSpan in one thread and deallocate in another. A simple way to ensure this
// is to only ever stack-allocate it.
//
// Example usage:
// {
//   WithSpan ws(span);
//   // Do work.
// }
class WithSpan {
 public:
  explicit WithSpan(const Span& span, bool cond = true, bool end_span = false);
  ~WithSpan();

  // No Span&& constructor because it encourages "consuming" the Span with a
  // std::move(). It's better to hold on to the Span object because we have to
  // call End() on it when the operation is finished.

 private:
  WithSpan() = delete;
  WithSpan(const WithSpan&) = delete;
  WithSpan(WithSpan&&) = delete;
  WithSpan& operator=(const WithSpan&) = delete;
  WithSpan& operator=(WithSpan&&) = delete;

  void ConditionalSwap();

  Span swapped_span_;
#ifndef NDEBUG
  const ::opencensus::context::Context* original_context_;
#endif
  const bool cond_;
  const bool end_span_;
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_WITH_SPAN_H_
