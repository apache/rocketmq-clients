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

#ifndef OPENCENSUS_TRACE_EXPORTER_SPAN_DATA_H_
#define OPENCENSUS_TRACE_EXPORTER_SPAN_DATA_H_

#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/strings/string_view.h"
#include "absl/time/time.h"
#include "opencensus/trace/exporter/annotation.h"
#include "opencensus/trace/exporter/attribute_value.h"
#include "opencensus/trace/exporter/link.h"
#include "opencensus/trace/exporter/message_event.h"
#include "opencensus/trace/exporter/status.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"

namespace opencensus {
namespace trace {
namespace exporter {

// SpanData represents a Span and its contents (annotations, etc). A Span can be
// converted to SpanData, and SpanData is passed to exporters. There is no plan
// other than using SpanData with exporting interfaces.
//
// SpanData can represent either a running span, or a span that has ended.
// SpanData is immutable.
//
// SpanData tries to match the Stackdriver v2 model:
// https://cloud.google.com/trace/docs/reference/v2/rpc/google.devtools.cloudtrace.v2
class SpanData final {
 public:
  // TimeEvent is a time-stamped event attached to the Span.
  template <typename T>
  class TimeEvent final {
   public:
    TimeEvent(absl::Time timestamp, T&& event)
        : timestamp_(timestamp), event_(std::move(event)) {}

    absl::Time timestamp() const { return timestamp_; }
    const T& event() const { return event_; }

   private:
    absl::Time timestamp_;
    T event_;
  };

  // TimeEvents is a collection of TimeEvents and the number of events that were
  // dropped.
  template <typename T>
  class TimeEvents final {
   public:
    TimeEvents(std::vector<TimeEvent<T>>&& events, int dropped_events_count)
        : events_(std::move(events)),
          dropped_events_count_(dropped_events_count) {}

    const std::vector<TimeEvent<T>>& events() const { return events_; }
    int dropped_events_count() const { return dropped_events_count_; }

   private:
    std::vector<TimeEvent<T>> events_;
    int dropped_events_count_;
  };

  // The constructor is visible for test purposes. Users are expected to get
  // SpanData from a Span object.
  SpanData(absl::string_view name, SpanContext context, SpanId parent_span_id,
           TimeEvents<Annotation>&& annotations,
           TimeEvents<MessageEvent>&& message_events, std::vector<Link>&& links,
           int num_links_dropped,
           std::unordered_map<std::string, AttributeValue>&& attributes,
           int num_attributes_dropped, bool has_ended, absl::Time start_time,
           absl::Time end_time, Status status, bool has_remote_parent);

  // --- Accessors ---

  // The name of the span, e.g. an RPC method name.
  absl::string_view name() const;

  // The SpanContext contains the trace_id, span_id, and options (IsSampled) for
  // the current span.
  SpanContext context() const;

  // The parent span's, span_id. This is all zeroes if the current span is a
  // root span.
  SpanId parent_span_id() const;

  // The annotations attached to this span.
  const TimeEvents<Annotation>& annotations() const;

  // Network events (send, recv) attached to this span, e.g. when an RPC reply
  // was sent.
  const TimeEvents<MessageEvent>& message_events() const;

  // Links to spans in other traces.
  const std::vector<Link>& links() const;

  // The number of links that were dropped.
  int num_links_dropped() const;

  // The attributes (key -> value) attached to this span, e.g. tags attached to
  // the current span.
  const std::unordered_map<std::string, AttributeValue>& attributes() const;

  // The number of attributes that were dropped.
  int num_attributes_dropped() const;

  // True if the span ended.
  bool has_ended() const;

  // The start time of the span.
  absl::Time start_time() const;

  // The end time of the span. Set to 0 if the span hasn't ended.
  absl::Time end_time() const;

  // The status of the span. Unset if the span hasn't ended.
  Status status() const;

  // True if the parent is on a different process.
  bool has_remote_parent() const;

  // Returns a human-readable string for debugging. Do not rely on its format or
  // try to parse it.
  std::string DebugString() const;

 private:
  std::string name_;
  SpanContext context_;
  SpanId parent_span_id_;
  TimeEvents<Annotation> annotations_;
  TimeEvents<MessageEvent> message_events_;
  std::vector<Link> links_;
  std::unordered_map<std::string, AttributeValue> attributes_;
  int num_links_dropped_;
  int num_attributes_dropped_;
  absl::Time start_time_;
  absl::Time end_time_;
  Status status_;
  bool has_remote_parent_;
  bool has_ended_;
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_EXPORTER_SPAN_DATA_H_
