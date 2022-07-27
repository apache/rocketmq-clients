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
  trace
  PUBLIC
  SRCS
  internal/annotation.cc
  internal/attribute_list.cc
  internal/attribute_value.cc
  internal/attribute_value_ref.cc
  internal/context_util.cc
  internal/link.cc
  internal/local_span_store.cc
  internal/local_span_store_impl.cc
  internal/message_event.cc
  internal/running_span_store.cc
  internal/running_span_store_impl.cc
  internal/sampler.cc
  internal/span.cc
  internal/span_data.cc
  internal/span_exporter.cc
  internal/span_exporter_impl.cc
  internal/span_impl.cc
  internal/status.cc
  internal/trace_config.cc
  internal/trace_config_impl.cc
  internal/with_span.cc
  DEPS
  common_random
  trace_cloud_trace_context
  trace_span_context
  trace_trace_context
  absl::strings
  absl::base
  absl::memory
  absl::synchronization
  absl::time
  absl::span)

# Define NOMINMAX to fix build errors when compiling with MSVC.
target_compile_definitions(opencensus_trace
                           PUBLIC $<$<CXX_COMPILER_ID:MSVC>:NOMINMAX>)

opencensus_lib(
  trace_b3
  PUBLIC
  SRCS
  internal/b3.cc
  DEPS
  trace_span_context
  absl::base
  absl::strings)

opencensus_lib(
  trace_cloud_trace_context
  PUBLIC
  SRCS
  internal/cloud_trace_context.cc
  DEPS
  trace_span_context
  absl::base
  absl::strings)

opencensus_lib(
  trace_context_util
  PUBLIC
  SRCS
  internal/context_util.cc
  DEPS
  trace
  context)

opencensus_lib(
  trace_grpc_trace_bin
  PUBLIC
  SRCS
  internal/grpc_trace_bin.cc
  DEPS
  trace_span_context
  absl::base
  absl::strings)

opencensus_lib(
  trace_span_context
  PUBLIC
  SRCS
  internal/span_context.cc
  internal/span_id.cc
  internal/trace_id.cc
  internal/trace_options.cc
  DEPS
  absl::strings)

opencensus_lib(
  trace_trace_context
  PUBLIC
  SRCS
  internal/trace_context.cc
  DEPS
  trace_span_context
  absl::base
  absl::strings)

opencensus_lib(
  trace_with_span
  PUBLIC
  SRCS
  internal/with_span.cc
  DEPS
  trace
  context)

# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------

opencensus_test(trace_annotation_test internal/annotation_test.cc trace)

opencensus_test(trace_attribute_value_ref_test
                internal/attribute_value_ref_test.cc trace)

opencensus_test(trace_attribute_value_test internal/attribute_value_test.cc
                trace)

opencensus_test(trace_b3_test internal/b3_test.cc trace_b3)

opencensus_test(trace_cloud_trace_context_test
                internal/cloud_trace_context_test.cc trace_cloud_trace_context)

opencensus_test(trace_context_util_test internal/context_util_test.cc trace
                trace_context_util trace_with_span context)

opencensus_test(trace_grpc_trace_bin_test internal/grpc_trace_bin_test.cc
                trace_grpc_trace_bin)

opencensus_test(trace_link_test internal/link_test.cc trace)

opencensus_test(trace_local_span_store_test internal/local_span_store_test.cc
                trace absl::memory absl::synchronization)

opencensus_test(
  trace_running_span_store_test internal/running_span_store_test.cc trace
  absl::base absl::memory absl::synchronization)

opencensus_test(trace_sampler_test internal/sampler_test.cc trace absl::strings
                absl::synchronization absl::time)

opencensus_test(trace_span_test internal/span_test.cc trace absl::strings)

opencensus_test(trace_span_id_test internal/span_id_test.cc trace)

opencensus_test(trace_span_options_test internal/span_options_test.cc trace
                absl::strings absl::synchronization)

opencensus_test(trace_span_context_test internal/span_context_test.cc
                trace_span_context absl::strings absl::span)

opencensus_test(
  trace_span_exporter_test
  internal/span_exporter_test.cc
  trace
  absl::memory
  absl::strings
  absl::synchronization
  absl::time)

opencensus_test(trace_status_test internal/status_test.cc trace absl::strings)

opencensus_test(trace_trace_config_test internal/trace_config_test.cc trace
                absl::time)

opencensus_test(trace_trace_options_test internal/trace_options_test.cc trace)

opencensus_test(trace_trace_context_test internal/trace_context_test.cc
                trace_trace_context)

opencensus_test(trace_with_span_test internal/with_span_test.cc trace
                trace_with_span context)

opencensus_benchmark(trace_attribute_value_ref_benchmark
                     internal/attribute_value_ref_benchmark.cc trace)

opencensus_benchmark(trace_b3_benchmark internal/b3_benchmark.cc trace_b3)

opencensus_benchmark(
  trace_cloud_trace_context_benchmark internal/cloud_trace_context_benchmark.cc
  trace_cloud_trace_context)

opencensus_benchmark(trace_grpc_trace_bin_benchmark
                     internal/grpc_trace_bin_benchmark.cc trace_grpc_trace_bin)

opencensus_benchmark(trace_sampler_benchmark internal/sampler_benchmark.cc
                     trace_span_context trace)

opencensus_benchmark(trace_span_benchmark internal/span_benchmark.cc
                     trace_span_context trace)

opencensus_benchmark(trace_span_id_benchmark internal/span_id_benchmark.cc
                     trace_span_context)

opencensus_benchmark(trace_context_benchmark
                     internal/trace_context_benchmark.cc trace_trace_context)

opencensus_benchmark(trace_with_span_benchmark internal/with_span_benchmark.cc
                     trace trace_with_span)

opencensus_fuzzer(trace_b3_fuzzer internal/b3_fuzzer.cc trace_b3 absl::strings)

opencensus_fuzzer(
  trace_cloud_trace_context_fuzzer internal/cloud_trace_context_fuzzer.cc
  trace_cloud_trace_context absl::strings)

opencensus_fuzzer(trace_grpc_trace_bin_fuzzer internal/grpc_trace_bin_fuzzer.cc
                  trace_grpc_trace_bin absl::strings)

opencensus_fuzzer(trace_trace_context_fuzzer internal/trace_context_fuzzer.cc
                  trace_trace_context absl::strings)
