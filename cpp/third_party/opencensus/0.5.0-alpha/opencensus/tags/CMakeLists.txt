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

opencensus_lib(
  tags
  PUBLIC
  SRCS
  internal/tag_key.cc
  internal/tag_map.cc
  DEPS
  absl::strings
  absl::base
  absl::hash
  absl::synchronization)

opencensus_lib(
  tags_context_util
  PUBLIC
  SRCS
  internal/context_util.cc
  DEPS
  tags
  context)

opencensus_lib(
  tags_grpc_tags_bin
  PUBLIC
  SRCS
  internal/grpc_tags_bin.cc
  DEPS
  tags
  common_varint
  absl::strings)

opencensus_lib(
  tags_with_tag_map
  PUBLIC
  SRCS
  internal/with_tag_map.cc
  DEPS
  tags
  context
  absl::strings)

opencensus_test(tags_context_util_test internal/context_util_test.cc tags
                tags_context_util tags_with_tag_map context)

opencensus_test(tags_grpc_tags_bin_test internal/grpc_tags_bin_test.cc tags
                tags_grpc_tags_bin)

opencensus_test(tags_tag_key_test internal/tag_key_test.cc tags)

opencensus_test(tags_tag_map_test internal/tag_map_test.cc tags)

opencensus_test(tags_with_tag_map_test internal/with_tag_map_test.cc tags
                tags_with_tag_map context)

opencensus_benchmark(
  tags_grpc_tags_bin_benchmark internal/grpc_tags_bin_benchmark.cc tags
  tags_grpc_tags_bin absl::strings)

opencensus_benchmark(tags_tag_map_benchmark internal/tag_map_benchmark.cc tags
                     absl::strings)

opencensus_fuzzer(tags_grpc_tags_bin_fuzzer internal/grpc_tags_bin_fuzzer.cc
                  tags_grpc_tags_bin absl::strings)
