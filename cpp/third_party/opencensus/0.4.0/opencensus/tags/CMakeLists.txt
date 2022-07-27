# Copyright 2018, OpenCensus Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

opencensus_lib(tags
               PUBLIC
               SRCS
               internal/tag_key.cc
               internal/tag_map.cc
               DEPS
               absl::strings
               common_hash_mix
               absl::base
               absl::synchronization)

opencensus_lib(tags_context_util
               PUBLIC
               SRCS
               internal/context_util.cc
               DEPS
               tags
               context)

opencensus_lib(tags_with_tag_map
               PUBLIC
               SRCS
               internal/with_tag_map.cc
               DEPS
               tags
               context
               absl::strings)