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

#ifndef OPENCENSUS_COMMON_INTERNAL_HOST_H_
#define OPENCENSUS_COMMON_INTERNAL_HOST_H_

#include <string>

namespace opencensus {
namespace common {

// Returns the current hostname or "unknown_hostname" on error.
std::string Hostname();

// Returns a default opencensus_task value for stats exporters, in
// the format "cpp-{PID}@{HOSTNAME}"
std::string OpenCensusTask();

}  // namespace common
}  // namespace opencensus

#endif  // OPENCENSUS_COMMON_INTERNAL_HOST_H_
