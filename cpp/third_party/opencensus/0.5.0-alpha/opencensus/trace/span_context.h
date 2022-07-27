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

#ifndef OPENCENSUS_TRACE_SPAN_CONTEXT_H_
#define OPENCENSUS_TRACE_SPAN_CONTEXT_H_

#include <cstdint>
#include <string>

#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_id.h"
#include "opencensus/trace/trace_options.h"

namespace opencensus {
namespace trace {

// SpanContext contains the state that must propagate to child spans and across
// process boundaries. It contains the TraceId and SpanId associated with the
// span and a set of TraceOptions. SpanContext is immutable.
class SpanContext final {
 public:
  // Blank SpanContext: TraceId, SpanId, and TraceOptions are 0.
  SpanContext() {}

  // SpanContext is copyable and movable.
  SpanContext(const SpanContext&) = default;
  SpanContext(SpanContext&&) = default;
  SpanContext& operator=(const SpanContext&) = default;
  SpanContext& operator=(SpanContext&&) = default;

  SpanContext(TraceId trace_id, SpanId span_id,
              TraceOptions trace_options = TraceOptions())
      : trace_id_(trace_id), span_id_(span_id), trace_options_(trace_options) {}

  // Returns the TraceId associated with this SpanContext.
  TraceId trace_id() const { return trace_id_; }

  // Returns the SpanId associated with this SpanContext.
  SpanId span_id() const { return span_id_; }

  // Returns the TraceOptions associated with this SpanContext.
  TraceOptions trace_options() const { return trace_options_; }

  // Compares two SpanContexts for equality. Only compares TraceId and SpanId,
  // not TraceOptions!
  bool operator==(const SpanContext& that) const;

  // Compares two SpanContexts for inequality. Only compares TraceId and SpanId,
  // not TraceOptions!
  bool operator!=(const SpanContext& that) const;

  // Returns true if SpanId and TraceId are valid.
  bool IsValid() const;

  // Returns hex strings separated with hyphens, e.g. "trace_id-span_id-options"
  std::string ToString() const;

 private:
  TraceId trace_id_;
  SpanId span_id_;
  TraceOptions trace_options_;
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_SPAN_CONTEXT_H_
