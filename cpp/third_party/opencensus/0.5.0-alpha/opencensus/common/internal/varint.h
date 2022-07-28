// Copyright 2019, OpenCensus Authors
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

#ifndef OPENCENSUS_COMMON_INTERNAL_VARINT_H_
#define OPENCENSUS_COMMON_INTERNAL_VARINT_H_

#include <cstdint>
#include <string>

#include "absl/strings/string_view.h"

namespace opencensus {
namespace common {

// Appends a variable-length encoded integer to the destination string.
void AppendVarint32(uint32_t i, std::string* out);

// Parses a variable-length encoded integer from the input. Returns false on
// failure. Returns true and consumes the bytes from the input, on success.
bool ParseVarint32(absl::string_view* input, uint32_t* out);

}  // namespace common
}  // namespace opencensus

#endif  // OPENCENSUS_COMMON_INTERNAL_VARINT_H_
