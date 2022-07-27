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

#ifndef OPENCENSUS_TRACE_INTERNAL_TRACE_EVENTS_H_
#define OPENCENSUS_TRACE_INTERNAL_TRACE_EVENTS_H_

#include <cstdint>
#include <deque>
#include <utility>

namespace opencensus {
namespace trace {

// A fixed size FIFO queue of events of type T.  T must have a valid copy
// constructor. TraceEvents is thread-compatible.
template <typename T>
class TraceEvents final {
 public:
  TraceEvents() : total_recorded_events_(0), max_events_(0) {}
  explicit TraceEvents(uint32_t max_events)
      : total_recorded_events_(0), max_events_(max_events) {}

  // Returns the number of the dropped events.
  uint32_t num_events_dropped() const;
  // Returns the number of the recorded events. Including the events that were
  // dropped.
  uint32_t num_events_recorded() const;

  // Adds an event to the event queue. If max_events_ is exceeded, an event
  // will be evicted in a FIFO manner.
  void AddEvent(const T& event);
  void AddEvent(T&& event);

  // Returns a vector of populate with all the events currently in the queue.
  const std::deque<T>& events() const;

 private:
  uint32_t total_recorded_events_;
  uint32_t max_events_;
  std::deque<T> events_;
};

template <typename T>
inline uint32_t TraceEvents<T>::num_events_dropped() const {
  return total_recorded_events_ - events_.size();
}

template <typename T>
inline uint32_t TraceEvents<T>::num_events_recorded() const {
  return num_events_recorded;
}

template <typename T>
inline void TraceEvents<T>::AddEvent(const T& event) {
  // Blank span has 0 max events.
  if (max_events_ == 0) {
    return;
  }

  if (events_.size() >= max_events_) {
    events_.pop_front();
  }
  events_.emplace_back(event);
  total_recorded_events_++;
}

template <typename T>
inline void TraceEvents<T>::AddEvent(T&& event) {
  // Blank span has 0 max events.
  if (max_events_ == 0) {
    return;
  }

  if (events_.size() >= max_events_) {
    events_.pop_front();
  }
  events_.emplace_back(std::move(event));
  total_recorded_events_++;
}

template <typename T>
inline const std::deque<T>& TraceEvents<T>::events() const {
  return events_;
}

}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_INTERNAL_TRACE_EVENTS_H_
