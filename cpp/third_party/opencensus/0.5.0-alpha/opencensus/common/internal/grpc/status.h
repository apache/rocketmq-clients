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

#ifndef OPENCENSUS_COMMON_INTERNAL_GRPC_STATUS_H_
#define OPENCENSUS_COMMON_INTERNAL_GRPC_STATUS_H_

#include <string>

#include <grpcpp/support/status.h>

namespace opencensus {
namespace common {

// Returns a string of the status code name and message.
std::string ToString(const grpc::Status& status);

}  // namespace common
}  // namespace opencensus

#endif  // OPENCENSUS_COMMON_INTERNAL_GRPC_STATUS_H_
