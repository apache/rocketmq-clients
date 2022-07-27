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

#include "benchmark/benchmark.h"
#include "opencensus/trace/span.h"
#include "opencensus/trace/span_context.h"

namespace {

void BM_StartEndSpan(benchmark::State& state) {
  static ::opencensus::trace::AlwaysSampler sampler;
  while (state.KeepRunning()) {
    auto span = ::opencensus::trace::Span::StartSpan(
        "SpanName", /*parent=*/nullptr, {&sampler});
    span.End();
  }
}
BENCHMARK(BM_StartEndSpan);

void BM_StartEndSpanAndAddAttribute(benchmark::State& state) {
  static ::opencensus::trace::AlwaysSampler sampler;
  while (state.KeepRunning()) {
    auto span = ::opencensus::trace::Span::StartSpan(
        "SpanName", /*parent=*/nullptr, {&sampler});
    span.AddAttribute("key1", "value1");
    span.End();
  }
}
BENCHMARK(BM_StartEndSpanAndAddAttribute);

void BM_StartEndSpanAndAddAnnotation(benchmark::State& state) {
  static ::opencensus::trace::AlwaysSampler sampler;
  while (state.KeepRunning()) {
    auto span = ::opencensus::trace::Span::StartSpan(
        "SpanName", /*parent=*/nullptr, {&sampler});
    span.AddAnnotation("This is an annotation.");
    span.End();
  }
}
BENCHMARK(BM_StartEndSpanAndAddAnnotation);

void BM_StartEndSpanAndAddMessageEvent(benchmark::State& state) {
  static ::opencensus::trace::AlwaysSampler sampler;
  while (state.KeepRunning()) {
    auto span = ::opencensus::trace::Span::StartSpan(
        "SpanName", /*parent=*/nullptr, {&sampler});
    span.AddSentMessageEvent(123, 456, 789);
    span.End();
  }
}
BENCHMARK(BM_StartEndSpanAndAddMessageEvent);

void BM_StartEndSpanAndAddLink(benchmark::State& state) {
  static ::opencensus::trace::AlwaysSampler sampler;
  constexpr uint8_t trace_id[] = {1, 2,  3,  4,  5,  6,  7,  8,
                                  9, 10, 11, 12, 13, 14, 15, 16};
  constexpr uint8_t span_id[] = {1, 2, 3, 4, 5, 6, 7, 8};
  ::opencensus::trace::SpanContext ctx{::opencensus::trace::TraceId(trace_id),
                                       ::opencensus::trace::SpanId(span_id)};
  while (state.KeepRunning()) {
    auto span = ::opencensus::trace::Span::StartSpan(
        "SpanName", /*parent=*/nullptr, {&sampler});
    span.AddParentLink(ctx);
    span.End();
  }
}
BENCHMARK(BM_StartEndSpanAndAddLink);

void BM_StartEndSpanAndSetStatus(benchmark::State& state) {
  static ::opencensus::trace::AlwaysSampler sampler;
  while (state.KeepRunning()) {
    auto span = ::opencensus::trace::Span::StartSpan(
        "SpanName", /*parent=*/nullptr, {&sampler});
    span.SetStatus(::opencensus::trace::StatusCode::CANCELLED,
                   "This is a description of the error.");
    span.End();
  }
}
BENCHMARK(BM_StartEndSpanAndSetStatus);

}  // namespace
BENCHMARK_MAIN();
