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

#include <cstdint>
#include <memory>
#include <string>
#include <utility>

#include "absl/strings/string_view.h"
#include "opencensus/common/internal/random.h"
#include "opencensus/trace/exporter/annotation.h"
#include "opencensus/trace/exporter/attribute_value.h"
#include "opencensus/trace/exporter/link.h"
#include "opencensus/trace/exporter/message_event.h"
#include "opencensus/trace/exporter/status.h"
#include "opencensus/trace/internal/local_span_store_impl.h"
#include "opencensus/trace/internal/running_span_store.h"
#include "opencensus/trace/internal/running_span_store_impl.h"
#include "opencensus/trace/internal/span_exporter_impl.h"
#include "opencensus/trace/internal/span_impl.h"
#include "opencensus/trace/internal/trace_config_impl.h"
#include "opencensus/trace/sampler.h"
#include "opencensus/trace/span.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/status_code.h"
#include "opencensus/trace/trace_id.h"
#include "opencensus/trace/trace_options.h"
#include "opencensus/trace/trace_params.h"

namespace opencensus {
namespace trace {
namespace {

// Generates a random SpanId.
SpanId GenerateRandomSpanId() {
  uint8_t span_id_buf[SpanId::kSize];
  ::opencensus::common::Random::GetRandom()->GenerateRandomBuffer(
      span_id_buf, SpanId::kSize);
  return SpanId(span_id_buf);
}

// Generates a random TraceId.
TraceId GenerateRandomTraceId() {
  uint8_t trace_id_buf[TraceId::kSize];
  ::opencensus::common::Random::GetRandom()->GenerateRandomBuffer(
      trace_id_buf, TraceId::kSize);
  return TraceId(trace_id_buf);
}

}  // namespace

class SpanGenerator {
 public:
  static Span Generate(absl::string_view name, const SpanContext* parent_ctx,
                       bool has_remote_parent,
                       const StartSpanOptions& options) {
    SpanId span_id = GenerateRandomSpanId();
    TraceId trace_id;
    SpanId parent_span_id;
    TraceOptions trace_options;
    if (parent_ctx == nullptr) {
      trace_id = GenerateRandomTraceId();
    } else {
      trace_id = parent_ctx->trace_id();
      parent_span_id = parent_ctx->span_id();
      trace_options = parent_ctx->trace_options();
    }
    if (!trace_options.IsSampled()) {
      bool should_sample = false;
      if (options.sampler != nullptr) {
        should_sample = options.sampler->ShouldSample(
            parent_ctx, has_remote_parent, trace_id, span_id, name,
            options.parent_links);
      } else {
        should_sample =
            TraceConfigImpl::Get()->current_trace_params().sampler.ShouldSample(
                parent_ctx, has_remote_parent, trace_id, span_id, name,
                options.parent_links);
      }
      trace_options = trace_options.WithSampling(should_sample);
    }
    SpanContext context(trace_id, span_id, trace_options);
    SpanImpl* impl = nullptr;
    if (trace_options.IsSampled()) {
      // Only Spans that are sampled are backed by a SpanImpl.
      impl =
          new SpanImpl(context, TraceConfigImpl::Get()->current_trace_params(),
                       name, parent_span_id, has_remote_parent);
    }
    // Add links.
    for (const auto& parent_link : options.parent_links) {
      if (impl) {
        impl->AddLink(parent_link->context(),
                      exporter::Link::Type::kParentLinkedSpan,
                      /*attributes=*/{});
      }
      parent_link->AddChildLink(context);
    }
    return Span(context, impl);
  }
};

Span Span::BlankSpan() { return Span(SpanContext(), nullptr); }

Span Span::StartSpan(absl::string_view name, const Span* parent,
                     const StartSpanOptions& options) {
  SpanContext parent_ctx;
  if (parent != nullptr) {
    parent_ctx = parent->context();
  }
  return SpanGenerator::Generate(name,
                                 (parent == nullptr) ? nullptr : &parent_ctx,
                                 /*has_remote_parent=*/false, options);
}

Span Span::StartSpanWithRemoteParent(absl::string_view name,
                                     const SpanContext& parent_ctx,
                                     const StartSpanOptions& options) {
  return SpanGenerator::Generate(name, &parent_ctx,
                                 /*has_remote_parent=*/true, options);
}

Span::Span(const SpanContext& context, SpanImpl* impl)
    : context_(context), span_impl_(impl) {
  if (IsRecording()) {
    exporter::RunningSpanStoreImpl::Get()->AddSpan(span_impl_);
  }
}

void Span::AddAttribute(absl::string_view key,
                        AttributeValueRef attribute) const {
  if (IsRecording()) {
    span_impl_->AddAttributes({{key, attribute}});
  }
}

void Span::AddAttributes(AttributesRef attributes) const {
  if (IsRecording()) {
    span_impl_->AddAttributes(attributes);
  }
}

void Span::AddAnnotation(absl::string_view description,
                         AttributesRef attributes) const {
  if (IsRecording()) {
    span_impl_->AddAnnotation(description, attributes);
  }
}

void Span::AddSentMessageEvent(uint32_t message_id,
                               uint32_t compressed_message_size,
                               uint32_t uncompressed_message_size) const {
  if (IsRecording()) {
    span_impl_->AddMessageEvent(exporter::MessageEvent::Type::SENT, message_id,
                                compressed_message_size,
                                uncompressed_message_size);
  }
}

void Span::AddReceivedMessageEvent(uint32_t message_id,
                                   uint32_t compressed_message_size,
                                   uint32_t uncompressed_message_size) const {
  if (IsRecording()) {
    span_impl_->AddMessageEvent(exporter::MessageEvent::Type::RECEIVED,
                                message_id, compressed_message_size,
                                uncompressed_message_size);
  }
}

void Span::AddParentLink(const SpanContext& parent_ctx,
                         AttributesRef attributes) const {
  if (IsRecording()) {
    span_impl_->AddLink(parent_ctx, exporter::Link::Type::kParentLinkedSpan,
                        attributes);
  }
}

void Span::AddChildLink(const SpanContext& child_ctx,
                        AttributesRef attributes) const {
  if (IsRecording()) {
    span_impl_->AddLink(child_ctx, exporter::Link::Type::kChildLinkedSpan,
                        attributes);
  }
}

void Span::SetStatus(StatusCode canonical_code,
                     absl::string_view message) const {
  if (IsRecording()) {
    span_impl_->SetStatus(exporter::Status(canonical_code, message));
  }
}

void Span::SetName(absl::string_view name) const {
  if (IsRecording()) {
    span_impl_->SetName(name);
  }
}

void Span::End() const {
  if (IsRecording()) {
    if (!span_impl_->End()) {
      // The Span already ended, ignore this call.
      return;
    }
    exporter::RunningSpanStoreImpl::Get()->RemoveSpan(span_impl_);
    exporter::LocalSpanStoreImpl::Get()->AddSpan(span_impl_);
    exporter::SpanExporterImpl::Get()->AddSpan(span_impl_);
  }
}

const SpanContext& Span::context() const { return context_; }

bool Span::IsSampled() const { return context_.trace_options().IsSampled(); }

bool Span::IsRecording() const { return span_impl_ != nullptr; }

void swap(Span& a, Span& b) {
  using std::swap;
  swap(a.context_, b.context_);
  swap(a.span_impl_, b.span_impl_);
}

}  // namespace trace
}  // namespace opencensus
