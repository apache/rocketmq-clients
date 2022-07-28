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

#include "opencensus/trace/internal/span_impl.h"

#include <deque>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/time/clock.h"
#include "opencensus/trace/attribute_value_ref.h"
#include "opencensus/trace/exporter/attribute_value.h"
#include "opencensus/trace/exporter/message_event.h"
#include "opencensus/trace/internal/local_span_store_impl.h"
#include "opencensus/trace/internal/running_span_store_impl.h"
#include "opencensus/trace/internal/span_exporter_impl.h"
#include "opencensus/trace/span.h"

namespace opencensus {
namespace trace {

namespace {
template <typename T>
std::vector<T> CopyTraceEvents(const std::deque<T>& events) {
  std::vector<T> trace_events;
  trace_events.reserve(events.size());
  for (const auto& event : events) {
    trace_events.emplace_back(event);
  }
  return trace_events;
}

template <typename T>
std::vector<exporter::SpanData::TimeEvent<T>> CopyEventWithTime(
    const std::deque<EventWithTime<T>>& events) {
  std::vector<exporter::SpanData::TimeEvent<T>> time_events;
  time_events.reserve(events.size());
  for (const auto& event : events) {
    auto tmp_event = event.event;
    time_events.emplace_back(event.time, std::move(tmp_event));
  }
  return time_events;
}

// Deep-copies an initializer_list of absl::string_view keys and
// AttributeValueRefs (cheap, used in the API) to an unordered_map that owns all
// of the data in it. If the same key appears multiple times, the last value
// wins.
std::unordered_map<std::string, exporter::AttributeValue> CopyAttributes(
    AttributesRef attributes) {
  std::unordered_map<std::string, exporter::AttributeValue> out;
  for (const auto& pair : attributes) {
    auto iter_inserted = out.insert(
        {std::string(pair.first), exporter::AttributeValue(pair.second)});
    if (!iter_inserted.second) {
      // Already exists, update.
      iter_inserted.first->second = exporter::AttributeValue(pair.second);
    }
  }
  return out;
}
}  // namespace

// SpanImpl::SpanImpl() : has_ended_(false), remote_parent_(false) {}

SpanImpl::SpanImpl(const SpanContext& context, const TraceParams& trace_params,
                   absl::string_view name, const SpanId& parent_span_id,
                   bool remote_parent)
    : start_time_(absl::Now()),
      name_(name),
      parent_span_id_(parent_span_id),
      context_(context),
      annotations_(trace_params.max_annotations),
      message_events_(trace_params.max_message_events),
      links_(trace_params.max_links),
      attributes_(trace_params.max_attributes),
      has_ended_(false),
      remote_parent_(remote_parent) {}

void SpanImpl::AddAttributes(AttributesRef attributes) {
  absl::MutexLock l(&mu_);
  if (!has_ended_) {
    for (const auto& attr : attributes) {
      attributes_.AddAttribute(attr.first,
                               exporter::AttributeValue(attr.second));
    }
  }
}

void SpanImpl::AddAnnotation(absl::string_view description,
                             AttributesRef attributes) {
  absl::MutexLock l(&mu_);
  if (!has_ended_) {
    annotations_.AddEvent(EventWithTime<exporter::Annotation>(
        absl::Now(),
        exporter::Annotation(description, CopyAttributes(attributes))));
  }
}

void SpanImpl::AddMessageEvent(exporter::MessageEvent::Type type,
                               uint32_t message_id,
                               uint32_t compressed_message_size,
                               uint32_t uncompressed_message_size) {
  absl::MutexLock l(&mu_);
  if (!has_ended_) {
    message_events_.AddEvent(EventWithTime<exporter::MessageEvent>(
        absl::Now(),
        exporter::MessageEvent(type, message_id, compressed_message_size,
                               uncompressed_message_size)));
  }
}

void SpanImpl::AddLink(const SpanContext& context, exporter::Link::Type type,
                       AttributesRef attributes) {
  absl::MutexLock l(&mu_);
  if (!has_ended_) {
    links_.AddEvent(exporter::Link(context, type, CopyAttributes(attributes)));
  }
}

void SpanImpl::SetStatus(exporter::Status&& status) {
  absl::MutexLock l(&mu_);
  if (!has_ended_) {
    status_ = std::move(status);
  }
}

void SpanImpl::SetName(absl::string_view name) {
  absl::MutexLock l(&mu_);
  if (!has_ended_) {
    name_ = std::string(name);
  }
}

bool SpanImpl::End() {
  absl::MutexLock l(&mu_);
  if (has_ended_) {
    assert(false && "Invalid attempt to End() the same Span more than once.");
    // In non-debug builds, just ignore the second End().
    return false;
  }
  has_ended_ = true;
  end_time_ = absl::Now();
  return true;
}

bool SpanImpl::HasEnded() const {
  absl::MutexLock l(&mu_);
  return has_ended_;
}

std::string SpanImpl::name() const {
  absl::MutexLock l(&mu_);
  return name_;
}

exporter::SpanData SpanImpl::ToSpanData() const {
  absl::MutexLock l(&mu_);
  // Make a deep copy of attributes.
  std::unordered_map<std::string, exporter::AttributeValue> attributes =
      attributes_.attributes();
  return exporter::SpanData(
      name_, context_, parent_span_id_,
      exporter::SpanData::TimeEvents<exporter::Annotation>(
          CopyEventWithTime(annotations_.events()),
          annotations_.num_events_dropped()),
      exporter::SpanData::TimeEvents<exporter::MessageEvent>(
          CopyEventWithTime(message_events_.events()),
          message_events_.num_events_dropped()),
      CopyTraceEvents(links_.events()), links_.num_events_dropped(),
      std::move(attributes), attributes_.num_attributes_dropped(), has_ended_,
      start_time_, end_time_, status_, remote_parent_);
}

}  // namespace trace
}  // namespace opencensus
