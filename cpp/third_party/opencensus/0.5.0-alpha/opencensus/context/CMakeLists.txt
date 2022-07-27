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
  context
  PUBLIC
  SRCS
  internal/context.cc
  internal/with_context.cc
  DEPS
  tags
  trace)

opencensus_test(
  context_context_test
  internal/context_test.cc
  context
  tags_context_util
  tags_with_tag_map
  trace_context_util
  trace_with_span)

opencensus_test(context_with_context_test internal/with_context_test.cc context)

opencensus_benchmark(context_context_benchmark internal/context_benchmark.cc
                     context)
