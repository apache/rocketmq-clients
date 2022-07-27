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

#include "opencensus/trace/internal/trace_config_impl.h"

#include <cstdint>

#include "opencensus/trace/sampler.h"
#include "opencensus/trace/trace_params.h"

namespace opencensus {
namespace trace {

namespace {
constexpr uint32_t kMaxAttributes = 32;
constexpr uint32_t kMaxAnnotations = 32;
constexpr uint32_t kMaxMessageEvents = 128;
constexpr uint32_t kMaxLinks = 32;
constexpr double kDefaultSamplingProbability = 1e-4;

TraceParams MakeDefaultTraceParams() {
  return TraceParams{kMaxAttributes, kMaxAnnotations, kMaxMessageEvents,
                     kMaxLinks,
                     ProbabilitySampler{kDefaultSamplingProbability}};
}
}  // namespace

TraceConfigImpl* TraceConfigImpl::Get() {
  static TraceConfigImpl* global_trace_params =
      new TraceConfigImpl(MakeDefaultTraceParams());
  return global_trace_params;
}

}  // namespace trace
}  // namespace opencensus
