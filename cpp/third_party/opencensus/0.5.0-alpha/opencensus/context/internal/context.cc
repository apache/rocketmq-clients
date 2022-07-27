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

#include "opencensus/context/context.h"

#include <functional>
#include <memory>
#include <utility>

#include "absl/strings/str_cat.h"
#include "opencensus/context/with_context.h"
#include "opencensus/tags/tag_map.h"
#include "opencensus/trace/span.h"

namespace opencensus {
namespace context {

// Wrapper for per-thread Context, frees the Context on thread shutdown.
class ContextWrapper {
 public:
  ContextWrapper() : ptr_(new Context) {}
  Context* get() { return ptr_.get(); }

 private:
  std::unique_ptr<Context> ptr_;
};

namespace {
thread_local ContextWrapper g_wrapper;
}  // namespace

Context::Context()
    : tags_(opencensus::tags::TagMap({})),
      span_(opencensus::trace::Span::BlankSpan()) {}

// static
const Context& Context::Current() { return *InternalMutableCurrent(); }

std::function<void()> Context::Wrap(std::function<void()> fn) const {
  Context copy(Context::Current());
  return [fn, copy]() {
    WithContext wc(copy);
    fn();
  };
}

std::string Context::DebugString() const {
  return absl::StrCat("ctx@", absl::Hex(this),
                      " span=", span_.context().ToString(),
                      ", tags=", tags_.DebugString());
}

// static
Context* Context::InternalMutableCurrent() { return g_wrapper.get(); }

void swap(Context& a, Context& b) {
  using std::swap;
  swap(a.span_, b.span_);
  swap(a.tags_, b.tags_);
}

}  // namespace context
}  // namespace opencensus
