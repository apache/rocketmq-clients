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

#ifndef OPENCENSUS_TRACE_EXPORTER_LINK_H_
#define OPENCENSUS_TRACE_EXPORTER_LINK_H_

#include <string>
#include <unordered_map>

#include "opencensus/trace/exporter/attribute_value.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_id.h"

namespace opencensus {
namespace trace {
namespace exporter {

// Represents a Link from one Span to a Span in another Trace. The linked Span
// can either be a parent or a child. Link is immutable.
class Link final {
 public:
  enum class Type : uint8_t {
    // When the linked Span is a child of the Span containing the link.
    kChildLinkedSpan,
    // When the linked Span is a parent of the Span containing the link.
    kParentLinkedSpan
  };

  Link(const SpanContext& context, Type link_type,
       std::unordered_map<std::string, AttributeValue> attributes =
           std::unordered_map<std::string, AttributeValue>())
      : type_(link_type),
        trace_id_(context.trace_id()),
        span_id_(context.span_id()),
        attributes_(std::move(attributes)) {}

  Type type() const { return type_; }

  TraceId trace_id() const { return trace_id_; }

  SpanId span_id() const { return span_id_; }

  const std::unordered_map<std::string, AttributeValue>& attributes() const {
    return attributes_;
  }

  // Returns a human-readable string for debugging. Do not rely on its format or
  // try to parse it.
  std::string DebugString() const;

 private:
  Type type_;
  TraceId trace_id_;
  SpanId span_id_;
  std::unordered_map<std::string, AttributeValue> attributes_;
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_EXPORTER_LINK_H_
