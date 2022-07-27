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

#include "opencensus/exporters/trace/stackdriver/stackdriver_exporter.h"

#include <cstdint>
#include <iostream>

#include <grpcpp/grpcpp.h>
#include "absl/base/macros.h"
#include "absl/memory/memory.h"
#include "absl/strings/str_cat.h"
#include "absl/time/clock.h"
#include "google/devtools/cloudtrace/v2/tracing.grpc.pb.h"
#include "opencensus/common/internal/grpc/status.h"
#include "opencensus/common/internal/grpc/with_user_agent.h"
#include "opencensus/common/internal/timestamp.h"
#include "opencensus/common/version.h"
#include "opencensus/trace/exporter/span_data.h"
#include "opencensus/trace/exporter/span_exporter.h"

namespace opencensus {
namespace exporters {
namespace trace {
namespace {

constexpr size_t kAttributeStringLen = 256;
constexpr size_t kAnnotationStringLen = 256;
constexpr size_t kDisplayNameStringLen = 128;
constexpr char kGoogleStackdriverTraceAddress[] = "cloudtrace.googleapis.com";

constexpr char kAgentKey[] = "g.co/agent";
constexpr char kAgentValue[] = "opencensus-cpp [" OPENCENSUS_VERSION "]";

void SetTruncatableString(
    absl::string_view str, size_t max_len,
    ::google::devtools::cloudtrace::v2::TruncatableString* t_str) {
  if (str.size() > max_len) {
    t_str->set_value(std::string(str.substr(0, max_len)));
    t_str->set_truncated_byte_count(str.size() - max_len);
  } else {
    t_str->set_value(std::string(str));
    t_str->set_truncated_byte_count(0);
  }
}

::google::devtools::cloudtrace::v2::Span_Link_Type ConvertLinkType(
    ::opencensus::trace::exporter::Link::Type type) {
  switch (type) {
    case ::opencensus::trace::exporter::Link::Type::kChildLinkedSpan:
      return ::google::devtools::cloudtrace::v2::
          Span_Link_Type_CHILD_LINKED_SPAN;
    case ::opencensus::trace::exporter::Link::Type::kParentLinkedSpan:
      return ::google::devtools::cloudtrace::v2::
          Span_Link_Type_PARENT_LINKED_SPAN;
  }
  return ::google::devtools::cloudtrace::v2::Span_Link_Type_TYPE_UNSPECIFIED;
}

::google::devtools::cloudtrace::v2::Span_TimeEvent_MessageEvent_Type
ConvertMessageType(::opencensus::trace::exporter::MessageEvent::Type type) {
  using Type = ::opencensus::trace::exporter::MessageEvent::Type;
  switch (type) {
    case Type::SENT:
      return ::google::devtools::cloudtrace::v2::
          Span_TimeEvent_MessageEvent_Type_SENT;
    case Type::RECEIVED:
      return ::google::devtools::cloudtrace::v2::
          Span_TimeEvent_MessageEvent_Type_RECEIVED;
  }
  return ::google::devtools::cloudtrace::v2::
      Span_TimeEvent_MessageEvent_Type_TYPE_UNSPECIFIED;
}

using AttributeMap =
    ::google::protobuf::Map<std::string,
                            ::google::devtools::cloudtrace::v2::AttributeValue>;
void PopulateAttributes(
    const std::unordered_map<
        std::string, ::opencensus::trace::exporter::AttributeValue>& attributes,
    AttributeMap* attribute_map) {
  for (const auto& attr : attributes) {
    using Type = ::opencensus::trace::exporter::AttributeValue::Type;
    switch (attr.second.type()) {
      case Type::kString:
        SetTruncatableString(
            attr.second.string_value(), kAttributeStringLen,
            (*attribute_map)[attr.first].mutable_string_value());
        break;
      case Type::kBool:
        (*attribute_map)[attr.first].set_bool_value(attr.second.bool_value());
        break;
      case Type::kInt:
        (*attribute_map)[attr.first].set_int_value(attr.second.int_value());
        break;
    }
  }
}

void ConvertAttributes(const ::opencensus::trace::exporter::SpanData& span,
                       ::google::devtools::cloudtrace::v2::Span* proto_span) {
  ::google::devtools::cloudtrace::v2::Span::Attributes attributes;
  PopulateAttributes(span.attributes(),
                     proto_span->mutable_attributes()->mutable_attribute_map());
  proto_span->mutable_attributes()->set_dropped_attributes_count(
      span.num_attributes_dropped());
}

void ConvertTimeEvents(const ::opencensus::trace::exporter::SpanData& span,
                       ::google::devtools::cloudtrace::v2::Span* proto_span) {
  for (const auto& annotation : span.annotations().events()) {
    auto event = proto_span->mutable_time_events()->add_time_event();
    opencensus::common::SetTimestamp(annotation.timestamp(),
                                     event->mutable_time());

    // Populate annotation.
    SetTruncatableString(annotation.event().description(), kAnnotationStringLen,
                         event->mutable_annotation()->mutable_description());
    PopulateAttributes(annotation.event().attributes(),
                       event->mutable_annotation()
                           ->mutable_attributes()
                           ->mutable_attribute_map());
  }

  for (const auto& message : span.message_events().events()) {
    auto event = proto_span->mutable_time_events()->add_time_event();
    opencensus::common::SetTimestamp(message.timestamp(),
                                     event->mutable_time());

    // Populate message event.
    event->mutable_message_event()->set_type(
        ConvertMessageType(message.event().type()));
    event->mutable_message_event()->set_id(message.event().id());
    event->mutable_message_event()->set_uncompressed_size_bytes(
        message.event().uncompressed_size());
    event->mutable_message_event()->set_compressed_size_bytes(
        message.event().compressed_size());
  }

  proto_span->mutable_time_events()->set_dropped_annotations_count(
      span.annotations().dropped_events_count());
  proto_span->mutable_time_events()->set_dropped_message_events_count(
      span.message_events().dropped_events_count());
}

void ConvertLinks(const ::opencensus::trace::exporter::SpanData& span,
                  ::google::devtools::cloudtrace::v2::Span* proto_span) {
  proto_span->mutable_links()->set_dropped_links_count(
      span.num_links_dropped());
  for (const auto& span_link : span.links()) {
    auto link = proto_span->mutable_links()->add_link();
    link->set_trace_id(span_link.trace_id().ToHex());
    link->set_span_id(span_link.span_id().ToHex());
    link->set_type(ConvertLinkType(span_link.type()));
    PopulateAttributes(
        span_link.attributes(),
        proto_span->mutable_attributes()->mutable_attribute_map());
  }
}

void ConvertSpans(
    const std::vector<::opencensus::trace::exporter::SpanData>& spans,
    absl::string_view project_id,
    ::google::devtools::cloudtrace::v2::BatchWriteSpansRequest* request) {
  for (const auto& from_span : spans) {
    auto to_span = request->add_spans();
    SetTruncatableString(from_span.name(), kDisplayNameStringLen,
                         to_span->mutable_display_name());
    to_span->set_name(absl::StrCat("projects/", project_id, "/traces/",
                                   from_span.context().trace_id().ToHex(),
                                   "/spans/",
                                   from_span.context().span_id().ToHex()));
    to_span->set_span_id(from_span.context().span_id().ToHex());
    to_span->set_parent_span_id(from_span.parent_span_id().ToHex());

    // The start time of the span.
    opencensus::common::SetTimestamp(from_span.start_time(),
                                     to_span->mutable_start_time());

    // The end time of the span.
    opencensus::common::SetTimestamp(from_span.end_time(),
                                     to_span->mutable_end_time());

    // Export Attributes
    ConvertAttributes(from_span, to_span);

    // Export Time Events.
    ConvertTimeEvents(from_span, to_span);

    // Export Links.
    ConvertLinks(from_span, to_span);

    // True if the parent is on a different process.
    to_span->mutable_same_process_as_parent_span()->set_value(
        !from_span.has_remote_parent());

    // The status of the span.
    to_span->mutable_status()->set_code(
        static_cast<int32_t>(from_span.status().CanonicalCode()));
    to_span->mutable_status()->set_message(from_span.status().error_message());

    // Add agent attribute.
    SetTruncatableString(
        kAgentValue, kAttributeStringLen,
        (*to_span->mutable_attributes()->mutable_attribute_map())[kAgentKey]
            .mutable_string_value());
  }
}

class Handler : public ::opencensus::trace::exporter::SpanExporter::Handler {
 public:
  Handler(StackdriverOptions&& opts) : opts_(std::move(opts)) {}

  void Export(const std::vector<::opencensus::trace::exporter::SpanData>& spans)
      override;

 private:
  const StackdriverOptions opts_;
};

void Handler::Export(
    const std::vector<::opencensus::trace::exporter::SpanData>& spans) {
  ::google::devtools::cloudtrace::v2::BatchWriteSpansRequest request;
  request.set_name(absl::StrCat("projects/", opts_.project_id));
  ConvertSpans(spans, opts_.project_id, &request);
  ::google::protobuf::Empty response;
  grpc::ClientContext context;
  context.set_deadline(absl::ToChronoTime(absl::Now() + opts_.rpc_deadline));
  opts_.prepare_client_context(&context);
  grpc::Status status =
      opts_.trace_service_stub->BatchWriteSpans(&context, request, &response);
  if (!status.ok()) {
    std::cerr << "BatchWriteSpans failed (" << spans.size() << " spans, "
              << request.ByteSizeLong()
              << " bytes): " << opencensus::common::ToString(status) << "\n";
  }
}

std::unique_ptr<google::devtools::cloudtrace::v2::TraceService::Stub>
MakeStackdriverStub() {
  auto channel = ::grpc::CreateCustomChannel(
      kGoogleStackdriverTraceAddress, ::grpc::GoogleDefaultCredentials(),
      ::opencensus::common::WithUserAgent());
  return ::google::devtools::cloudtrace::v2::TraceService::NewStub(channel);
}

}  // namespace

// static
void StackdriverExporter::Register(StackdriverOptions&& opts) {
  if (opts.trace_service_stub == nullptr) {
    opts.trace_service_stub = MakeStackdriverStub();
  }
  ::opencensus::trace::exporter::SpanExporter::RegisterHandler(
      absl::make_unique<Handler>(std::move(opts)));
}

// static, DEPRECATED
void StackdriverExporter::Register(StackdriverOptions& opts) {
  if (opts.trace_service_stub == nullptr) {
    opts.trace_service_stub = MakeStackdriverStub();
  }
  // Copy opts but take ownership of trace_service_stub.
  StackdriverOptions copied_opts;
  copied_opts.project_id = opts.project_id;
  copied_opts.rpc_deadline = opts.rpc_deadline;
  copied_opts.trace_service_stub = std::move(opts.trace_service_stub);
  ::opencensus::trace::exporter::SpanExporter::RegisterHandler(
      absl::make_unique<Handler>(std::move(copied_opts)));
}

// static, DEPRECATED
void StackdriverExporter::Register(absl::string_view project_id) {
  StackdriverOptions opts;
  opts.project_id = std::string(project_id);
  Register(std::move(opts));
}

}  // namespace trace
}  // namespace exporters
}  // namespace opencensus
