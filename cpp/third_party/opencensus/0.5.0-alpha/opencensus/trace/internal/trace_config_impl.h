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

#ifndef OPENCENSUS_TRACE_INTERNAL_TRACE_CONFIG_IMPL_H_
#define OPENCENSUS_TRACE_INTERNAL_TRACE_CONFIG_IMPL_H_

#include <memory>

#include "opencensus/trace/internal/trace_params_impl.h"
#include "opencensus/trace/trace_config.h"
#include "opencensus/trace/trace_params.h"

namespace opencensus {
namespace trace {

// TraceConfigImpl is a singleton that implements the TraceConfig interface.
// It's thread-safe.
class TraceConfigImpl {
 public:
  // Returns the singleton.
  static TraceConfigImpl* Get();

  void SetCurrentTraceParams(const TraceParams& params) {
    current_trace_params_.Set(params);
  }

  TraceParams current_trace_params() const {
    return current_trace_params_.Get();
  }

 private:
  TraceConfigImpl(const TraceParams& params) : current_trace_params_(params) {}

  TraceParamsImpl current_trace_params_;
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_INTERNAL_TRACE_CONFIG_IMPL_H_
