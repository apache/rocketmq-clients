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

opencensus_lib(stats PUBLIC DEPS stats_core stats_recording)

opencensus_lib(
  stats_test_utils
  PUBLIC
  SRCS
  testing/test_utils.cc
  DEPS
  stats_core
  absl::memory
  absl::time)

opencensus_lib(
  stats_core
  SRCS
  internal/aggregation.cc
  internal/aggregation_window.cc
  internal/bucket_boundaries.cc
  internal/delta_producer.cc
  internal/distribution.cc
  internal/measure.cc
  internal/measure_data.cc
  internal/measure_descriptor.cc
  internal/measure_registry.cc
  internal/measure_registry_impl.cc
  internal/set_aggregation_window.cc
  internal/stats_exporter.cc
  internal/stats_manager.cc
  internal/view.cc
  internal/view_data.cc
  internal/view_data_impl.cc
  internal/view_descriptor.cc
  DEPS
  absl::base
  common_stats_object
  common_string_vector_hash
  tags
  absl::memory
  absl::strings
  absl::synchronization
  absl::time
  absl::optional
  absl::span)

# Define NOMINMAX to fix build errors when compiling with MSVC.
target_compile_definitions(opencensus_stats_core
                           PUBLIC $<$<CXX_COMPILER_ID:MSVC>:NOMINMAX>)

opencensus_lib(
  stats_recording
  SRCS
  internal/recording.cc
  DEPS
  stats_core
  tags
  tags_context_util
  absl::strings
  absl::time)

# ----------------------------------------------------------------------
# Tests
# ----------------------------------------------------------------------

opencensus_test(stats_debug_string_test internal/debug_string_test.cc
                stats_core absl::time)

opencensus_test(stats_distribution_test internal/distribution_test.cc
                stats_core stats_test_utils)

opencensus_test(stats_bucket_boundaries_test internal/bucket_boundaries_test.cc
                stats_core)

opencensus_test(stats_measure_data_test internal/measure_data_test.cc
                stats_core stats_test_utils absl::span)

opencensus_test(stats_measure_registry_test internal/measure_registry_test.cc
                stats_core absl::strings)

opencensus_test(stats_stats_exporter_test internal/stats_exporter_test.cc
                stats_core stats_recording absl::memory absl::time)

opencensus_test(
  stats_stats_manager_test
  internal/stats_manager_test.cc
  stats_core
  stats_recording
  stats_test_utils
  tags
  tags_with_tag_map)

opencensus_test(stats_view_data_impl_test internal/view_data_impl_test.cc
                stats_core absl::time)

opencensus_benchmark(
  stats_stats_manager_benchmark
  internal/stats_manager_benchmark.cc
  stats_core
  stats_recording
  absl::memory
  absl::strings
  absl::time)
