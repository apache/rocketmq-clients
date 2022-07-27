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

#ifndef OPENCENSUS_TRACE_INTERNAL_EVENT_WITH_TIME_H_
#define OPENCENSUS_TRACE_INTERNAL_EVENT_WITH_TIME_H_

#include "absl/time/time.h"

namespace opencensus {
namespace trace {

// Event with a timestamp.
template <typename T>
struct EventWithTime {
  EventWithTime(absl::Time record_time, const T& record_event)
      : time(record_time), event(record_event) {}
  EventWithTime(absl::Time record_time, T&& record_event)
      : time(record_time), event(std::move(record_event)) {}

  absl::Time time;
  T event;
};

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_INTERNAL_EVENT_WITH_TIME_H_
