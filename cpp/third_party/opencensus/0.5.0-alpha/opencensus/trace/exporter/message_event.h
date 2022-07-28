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

#ifndef OPENCENSUS_TRACE_EXPORTER_MESSAGE_EVENT_H_
#define OPENCENSUS_TRACE_EXPORTER_MESSAGE_EVENT_H_

#include <cstdint>
#include <string>

namespace opencensus {
namespace trace {
namespace exporter {

// MessageEvent represents a message sent/received between Spans. It requires a
// Type and a message ID that can be used to match sent and received
// MessageEvents. It can optionally have information about the message size.
// MessageEvent is immutable.
class MessageEvent final {
 public:
  enum class Type : uint8_t {
    // The message was sent.
    SENT = 1,
    // The message was received.
    RECEIVED = 2,
  };

  MessageEvent(Type type, uint32_t id, uint32_t compressed_size,
               uint32_t uncompressed_size)
      : type_(type),
        id_(id),
        compressed_size_(compressed_size),
        uncompressed_size_(uncompressed_size) {}

  Type type() const { return type_; }
  uint32_t id() const { return id_; }
  uint32_t compressed_size() const { return compressed_size_; }
  uint32_t uncompressed_size() const { return uncompressed_size_; }

  // Returns a human-readable string for debugging. Do not rely on its format or
  // try to parse it.
  std::string DebugString() const;

 private:
  Type type_;
  uint32_t id_;
  uint32_t compressed_size_;
  uint32_t uncompressed_size_;
};

}  // namespace exporter
}  // namespace trace
}  // namespace opencensus

#endif  // OPENCENSUS_TRACE_EXPORTER_MESSAGE_EVENT_H_
