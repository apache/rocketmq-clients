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

#include "opencensus/trace/context_util.h"

#include "opencensus/context/context.h"
#include "opencensus/trace/span.h"

using ::opencensus::context::Context;

namespace opencensus {
namespace trace {

class ContextPeer {
 public:
  static const Span& GetSpanFromContext(const Context& ctx) {
    return ctx.span_;
  }
};

const Span& GetCurrentSpan() { return GetSpanFromContext(Context::Current()); }

const Span& GetSpanFromContext(const Context& ctx) {
  return ContextPeer::GetSpanFromContext(ctx);
}

}  // namespace trace
}  // namespace opencensus
