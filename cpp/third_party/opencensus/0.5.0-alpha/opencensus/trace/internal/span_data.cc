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

#include "opencensus/trace/exporter/span_data.h"

#include <string>
#include <utility>

#include "absl/strings/str_cat.h"

namespace opencensus {
namespace trace {
namespace exporter {

using absl::StrAppend;

SpanData::SpanData(absl::string_view name, SpanContext context,
                   SpanId parent_span_id, TimeEvents<Annotation>&& annotations,
                   TimeEvents<MessageEvent>&& message_events,
                   std::vector<Link>&& links, int num_links_dropped,
                   std::unordered_map<std::string, AttributeValue>&& attributes,
                   int num_attributes_dropped, bool has_ended,
                   absl::Time start_time, absl::Time end_time, Status status,
                   bool has_remote_parent)
    : name_(name),
      context_(context),
      parent_span_id_(parent_span_id),
      annotations_(std::move(annotations)),
      message_events_(std::move(message_events)),
      links_(std::move(links)),
      attributes_(std::move(attributes)),
      num_links_dropped_(num_links_dropped),
      num_attributes_dropped_(num_attributes_dropped),
      start_time_(start_time),
      end_time_(end_time),
      status_(std::move(status)),
      has_remote_parent_(has_remote_parent),
      has_ended_(has_ended) {}

absl::string_view SpanData::name() const { return name_; }

SpanContext SpanData::context() const { return context_; }

SpanId SpanData::parent_span_id() const { return parent_span_id_; }

const SpanData::TimeEvents<Annotation>& SpanData::annotations() const {
  return annotations_;
}

const SpanData::TimeEvents<MessageEvent>& SpanData::message_events() const {
  return message_events_;
}

const std::vector<Link>& SpanData::links() const { return links_; }

int SpanData::num_links_dropped() const { return num_links_dropped_; }

const std::unordered_map<std::string, AttributeValue>& SpanData::attributes()
    const {
  return attributes_;
}

int SpanData::num_attributes_dropped() const { return num_attributes_dropped_; }

bool SpanData::has_ended() const { return has_ended_; }

absl::Time SpanData::start_time() const { return start_time_; }

absl::Time SpanData::end_time() const { return end_time_; }

Status SpanData::status() const { return status_; }

bool SpanData::has_remote_parent() const { return has_remote_parent_; }

std::string SpanData::DebugString() const {
  std::string debug_str;
  StrAppend(&debug_str, "Name: ", name(),
            "\nTraceId-SpanId-Options: ", context().ToString(),
            "\nParent SpanId: ", parent_span_id().ToHex(),
            " (remote: ", has_remote_parent() ? "true" : "false",
            ")\nStart time: ", absl::FormatTime(start_time()),
            "\nEnd time: ", absl::FormatTime(end_time()), "\n");

  StrAppend(&debug_str, "Attributes: (", num_attributes_dropped(),
            " dropped)\n");
  for (const auto& attribute : attributes()) {
    StrAppend(&debug_str, "  \"", attribute.first,
              "\":", attribute.second.DebugString(), "\n");
  }

  StrAppend(&debug_str, "Annotations: (", annotations().dropped_events_count(),
            " dropped)\n");
  for (const auto& annotation : annotations().events()) {
    StrAppend(&debug_str, "  ", absl::FormatTime(annotation.timestamp()), ": ",
              annotation.event().DebugString(), "\n");
  }

  StrAppend(&debug_str, "Message events: (",
            message_events().dropped_events_count(), " dropped)\n");
  for (const auto& message : message_events().events()) {
    StrAppend(&debug_str, "  ", absl::FormatTime(message.timestamp()), ": ",
              message.event().DebugString(), "\n");
  }

  StrAppend(&debug_str, "Links: (", num_links_dropped(), " dropped)\n");
  for (const auto& link : links()) {
    StrAppend(&debug_str, "  ", link.DebugString(), "\n");
  }

  StrAppend(&debug_str, "Span ended: ", (has_ended() ? "true" : "false"), "\n");

  StrAppend(&debug_str, "Status: ", status().ToString(), "\n");
  return debug_str;
}

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus
