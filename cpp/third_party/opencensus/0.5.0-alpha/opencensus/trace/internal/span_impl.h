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

#ifndef OPENCENSUS_TRACE_INTERNAL_SPAN_IMPL_H_
#define OPENCENSUS_TRACE_INTERNAL_SPAN_IMPL_H_

#include <string>
#include <unordered_map>

#include "absl/base/thread_annotations.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "absl/time/time.h"
#include "opencensus/trace/exporter/annotation.h"
#include "opencensus/trace/exporter/attribute_value.h"
#include "opencensus/trace/exporter/link.h"
#include "opencensus/trace/exporter/message_event.h"
#include "opencensus/trace/exporter/span_data.h"
#include "opencensus/trace/exporter/status.h"
#include "opencensus/trace/internal/attribute_list.h"
#include "opencensus/trace/internal/event_with_time.h"
#include "opencensus/trace/internal/trace_events.h"
#include "opencensus/trace/span.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_config.h"
#include "opencensus/trace/trace_params.h"

namespace opencensus {
namespace trace {

namespace exporter {
class LocalSpanStoreImpl;
class RunningSpanStoreImpl;
class SpanExporterImpl;
}  // namespace exporter

class SpanTestPeer;

// SpanImpl is the underlying representation of a Span. Span has a shared_ptr
// that points to a SpanImpl. Multiple Spans, stores, and exporters can share
// a single SpanImpl.
//
// This is not a public API, please refer to ../span.h.
//
// SpanImpl is thread-safe.
class SpanImpl final {
 public:
  SpanImpl() = delete;
  SpanImpl(const SpanImpl&) = delete;
  SpanImpl(SpanImpl&&) = delete;
  SpanImpl& operator=(const SpanImpl&) = delete;
  SpanImpl& operator=(SpanImpl&&) = delete;

  // SpanContext sets the TraceId, SpanId, and TraceOptions for the span.
  // TraceParams sets the maximum number of attributes, annotations, network
  // events, and links. The name allows for a user provided description of the
  // span.
  SpanImpl(const SpanContext& context, const TraceParams& trace_params,
           absl::string_view name, const SpanId& parent_span_id,
           bool remote_parent);

  void AddAttributes(AttributesRef attributes) ABSL_LOCKS_EXCLUDED(mu_);

  void AddAnnotation(absl::string_view description, AttributesRef attributes)
      ABSL_LOCKS_EXCLUDED(mu_);

  void AddMessageEvent(exporter::MessageEvent::Type type, uint32_t message_id,
                       uint32_t compressed_message_size,
                       uint32_t uncompressed_message_size);

  void AddLink(const SpanContext& context, exporter::Link::Type type,
               AttributesRef attributes) ABSL_LOCKS_EXCLUDED(mu_);

  void SetStatus(exporter::Status&& status) ABSL_LOCKS_EXCLUDED(mu_);

  void SetName(absl::string_view name) ABSL_LOCKS_EXCLUDED(mu_);

  // Returns true on success (if this is the first time the Span has ended) and
  // also marks the end of the Span and sets its end_time_.
  bool End() ABSL_LOCKS_EXCLUDED(mu_);

  // Returns true if the span has ended.
  bool HasEnded() const ABSL_LOCKS_EXCLUDED(mu_);

  // Returns a copy of the current name of the Span, since SetName can be used
  // to change it.
  std::string name() const ABSL_LOCKS_EXCLUDED(mu_);

  // Returns the SpanContext associated with this Span.
  SpanContext context() const { return context_; }

  SpanId parent_span_id() const { return parent_span_id_; }

 private:
  friend class ::opencensus::trace::exporter::RunningSpanStoreImpl;
  friend class ::opencensus::trace::exporter::LocalSpanStoreImpl;
  friend class ::opencensus::trace::exporter::SpanExporterImpl;
  friend class ::opencensus::trace::SpanTestPeer;

  // Makes a deep copy of span contents and returns copied data in SpanData.
  exporter::SpanData ToSpanData() const ABSL_LOCKS_EXCLUDED(mu_);

  mutable absl::Mutex mu_;
  // The start time of the span.
  const absl::Time start_time_;
  // The end time of the span. Set when End() is called.
  absl::Time end_time_ ABSL_GUARDED_BY(mu_);
  // The status of the span. Only set if start_options_.record_events is true.
  exporter::Status status_ ABSL_GUARDED_BY(mu_);
  // The displayed name of the span.
  std::string name_ ABSL_GUARDED_BY(mu_);
  // The parent SpanId of this span. Parent SpanId will be not valid if this is
  // a root span.
  const SpanId parent_span_id_;
  // TraceId, SpanId, and TraceOptions for the current span.
  const SpanContext context_;
  // Queue of recorded annotations.
  TraceEvents<EventWithTime<exporter::Annotation>> annotations_
      ABSL_GUARDED_BY(mu_);
  // Queue of recorded network events.
  TraceEvents<EventWithTime<exporter::MessageEvent>> message_events_
      ABSL_GUARDED_BY(mu_);
  // Queue of recorded links to parent and child spans.
  TraceEvents<exporter::Link> links_ ABSL_GUARDED_BY(mu_);
  // Set of recorded attributes.
  AttributeList attributes_ ABSL_GUARDED_BY(mu_);
  // Marks if the span has ended.
  bool has_ended_ ABSL_GUARDED_BY(mu_);
  // True if the parent Span is in a different process.
  const bool remote_parent_;
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_INTERNAL_SPAN_IMPL_H_
