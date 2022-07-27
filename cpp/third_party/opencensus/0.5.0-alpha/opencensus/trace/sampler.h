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

#ifndef OPENCENSUS_TRACE_SAMPLER_H_
#define OPENCENSUS_TRACE_SAMPLER_H_

#include <cstdint>
#include <vector>

#include "absl/base/attributes.h"
#include "absl/strings/string_view.h"
#include "opencensus/trace/span_context.h"
#include "opencensus/trace/span_id.h"
#include "opencensus/trace/trace_id.h"

namespace opencensus {
namespace trace {

class Span;

// Samplers decide whether or not a given Span will be sampled.
// All implementations of Sampler must be thread-safe!
class Sampler {
 public:
  virtual ~Sampler() = default;

  // Returns true if a Span with the following data should be sampled. If there
  // is no parent, then parent_context will be nullptr. The lifetime of the
  // parent_context has to be valid for the duration of the ShouldSample() call.
  // The Sampler must not hold on to the pointer.
  virtual bool ShouldSample(const SpanContext* parent_context,
                            bool has_remote_parent, const TraceId& trace_id,
                            const SpanId& span_id, absl::string_view name,
                            const std::vector<Span*>& parent_links) const = 0;
};

// Returns true or false for sampling based on the given probability. Objects of
// this class should be cached between uses because there is a cost to
// constructing them.
class ProbabilitySampler final : public Sampler {
 public:
  explicit ProbabilitySampler(double probability);

  bool ShouldSample(const SpanContext* parent_context, bool has_remote_parent,
                    const TraceId& trace_id, const SpanId& span_id,
                    absl::string_view name,
                    const std::vector<Span*>& parent_links) const override;

 private:
  friend class TraceParamsImpl;  // For the global ProbabilitySampler.
  explicit ProbabilitySampler(uint64_t threshold) : threshold_(threshold) {}

  // Probability is converted to a value between [0, UINT64_MAX].
  const uint64_t threshold_;
};

// Always samples.
class AlwaysSampler final : public Sampler {
 public:
  bool ShouldSample(const SpanContext* parent_context ABSL_ATTRIBUTE_UNUSED,
                    bool has_remote_parent ABSL_ATTRIBUTE_UNUSED,
                    const TraceId& trace_id ABSL_ATTRIBUTE_UNUSED,
                    const SpanId& span_id ABSL_ATTRIBUTE_UNUSED,
                    absl::string_view name ABSL_ATTRIBUTE_UNUSED,
                    const std::vector<Span*>& parent_links
                        ABSL_ATTRIBUTE_UNUSED) const override {
    return true;
  }
};

// Never samples.
class NeverSampler final : public Sampler {
 public:
  bool ShouldSample(const SpanContext* parent_context ABSL_ATTRIBUTE_UNUSED,
                    bool has_remote_parent ABSL_ATTRIBUTE_UNUSED,
                    const TraceId& trace_id ABSL_ATTRIBUTE_UNUSED,
                    const SpanId& span_id ABSL_ATTRIBUTE_UNUSED,
                    absl::string_view name ABSL_ATTRIBUTE_UNUSED,
                    const std::vector<Span*>& parent_links
                        ABSL_ATTRIBUTE_UNUSED) const override {
    return false;
  }
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_SAMPLER_H_
