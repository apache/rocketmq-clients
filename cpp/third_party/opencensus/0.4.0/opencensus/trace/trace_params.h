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

#ifndef OPENCENSUS_TRACE_TRACE_PARAMS_H_
#define OPENCENSUS_TRACE_TRACE_PARAMS_H_

#include <cstdint>

#include "opencensus/trace/sampler.h"

namespace opencensus {
namespace trace {

// TraceParams holds the limits for attributes, annotations, message_events,
// links, and a ProbabilitySampler. For performance, only ProbabilitySampler is
// supported as the globally active sampler.
//
// The currently active TraceParams is set in TraceConfig.
struct TraceParams final {
  uint32_t max_attributes;
  uint32_t max_annotations;
  uint32_t max_message_events;
  uint32_t max_links;
  ProbabilitySampler sampler;
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_TRACE_PARAMS_H_
