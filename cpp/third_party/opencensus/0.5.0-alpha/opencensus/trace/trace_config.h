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

#ifndef OPENCENSUS_TRACE_TRACE_CONFIG_H_
#define OPENCENSUS_TRACE_TRACE_CONFIG_H_

#include "opencensus/trace/trace_params.h"

namespace opencensus {
namespace trace {

// TraceConfig holds the currently active TraceParams.
// TraceConfig is thread-safe.
class TraceConfig {
 public:
  // Sets the currently active TraceParams. Doing this is not atomic: individual
  // parts of the active TraceParams are updated separately.
  static void SetCurrentTraceParams(const TraceParams& params);
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_TRACE_CONFIG_H_
