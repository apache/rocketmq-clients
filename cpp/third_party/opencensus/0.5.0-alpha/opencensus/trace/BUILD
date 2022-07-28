# OpenCensus C++ Trace library.
# See ../../README.md for details.
#
# Copyright 2017, OpenCensus Authors
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

load("//opencensus:copts.bzl", "DEFAULT_COPTS", "TEST_COPTS")
load("//bazel:cc_fuzz_target.bzl", "cc_fuzz_target")

licenses(["notice"])  # Apache 2.0

package(default_visibility = ["//visibility:private"])

cc_library(
    name = "trace",
    srcs = [
        "internal/annotation.cc",
        "internal/attribute_list.cc",
        "internal/attribute_value.cc",
        "internal/attribute_value_ref.cc",
        "internal/event_with_time.h",
        "internal/link.cc",
        "internal/local_span_store.cc",
        "internal/local_span_store_impl.cc",
        "internal/message_event.cc",
        "internal/running_span_store.cc",
        "internal/running_span_store_impl.cc",
        "internal/sampler.cc",
        "internal/span.cc",
        "internal/span_data.cc",
        "internal/span_exporter.cc",
        "internal/span_exporter_impl.cc",
        "internal/span_impl.cc",
        "internal/status.cc",
        "internal/trace_config.cc",
        "internal/trace_config_impl.cc",
    ],
    hdrs = [
        "attribute_value_ref.h",
        "exporter/annotation.h",
        "exporter/attribute_value.h",
        "exporter/link.h",
        "exporter/message_event.h",
        "exporter/span_data.h",
        "exporter/span_exporter.h",
        "exporter/status.h",
        "internal/attribute_list.h",
        "internal/local_span_store.h",
        "internal/local_span_store_impl.h",
        "internal/running_span_store.h",
        "internal/running_span_store_impl.h",
        "internal/span_exporter_impl.h",
        "internal/span_impl.h",
        "internal/trace_config_impl.h",
        "internal/trace_events.h",
        "internal/trace_params_impl.h",
        "sampler.h",
        "span.h",
        "status_code.h",
        "trace_config.h",
        "trace_params.h",
    ],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":cloud_trace_context",
        ":span_context",
        ":trace_context",
        "//opencensus/common/internal:random_lib",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/base:endian",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "b3",
    srcs = [
        "internal/b3.cc",
    ],
    hdrs = [
        "propagation/b3.h",
    ],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":span_context",
        "@com_google_absl//absl/base:endian",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "cloud_trace_context",
    srcs = [
        "internal/cloud_trace_context.cc",
    ],
    hdrs = [
        "propagation/cloud_trace_context.h",
    ],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":span_context",
        "@com_google_absl//absl/base:endian",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "context_util",
    srcs = ["internal/context_util.cc"],
    hdrs = ["context_util.h"],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":trace",
        "//opencensus/context",
    ],
)

cc_library(
    name = "grpc_trace_bin",
    srcs = [
        "internal/grpc_trace_bin.cc",
    ],
    hdrs = [
        "propagation/grpc_trace_bin.h",
    ],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":span_context",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "span_context",
    srcs = [
        "internal/span_context.cc",
        "internal/span_id.cc",
        "internal/trace_id.cc",
        "internal/trace_options.cc",
    ],
    hdrs = [
        "span_context.h",
        "span_id.h",
        "trace_id.h",
        "trace_options.h",
    ],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "trace_context",
    srcs = [
        "internal/trace_context.cc",
    ],
    hdrs = [
        "propagation/trace_context.h",
    ],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":span_context",
        "@com_google_absl//absl/base:endian",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "with_span",
    srcs = ["internal/with_span.cc"],
    hdrs = ["with_span.h"],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":trace",
        "//opencensus/context",
    ],
)

# Tests
# ========================================================================= #

cc_test(
    name = "annotation_test",
    srcs = ["internal/annotation_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":trace",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "attribute_value_ref_test",
    srcs = ["internal/attribute_value_ref_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":trace",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "attribute_value_test",
    srcs = ["internal/attribute_value_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":trace",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "b3_test",
    srcs = ["internal/b3_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":b3",
        ":span_context",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "cloud_trace_context_test",
    srcs = ["internal/cloud_trace_context_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":cloud_trace_context",
        ":span_context",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "context_util_test",
    srcs = ["internal/context_util_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":context_util",
        ":span_context",
        ":trace",
        ":with_span",
        "//opencensus/context",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "grpc_trace_bin_test",
    srcs = ["internal/grpc_trace_bin_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":grpc_trace_bin",
        ":span_context",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "link_test",
    srcs = ["internal/link_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":span_context",
        ":trace",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "local_span_store_test",
    srcs = ["internal/local_span_store_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":trace",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "running_span_store_test",
    srcs = ["internal/running_span_store_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":span_context",
        ":trace",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "sampler_test",
    srcs = ["internal/sampler_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":trace",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "span_test",
    srcs = ["internal/span_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":span_context",
        ":trace",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "span_id_test",
    srcs = ["internal/span_id_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":span_context",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "span_options_test",
    srcs = ["internal/span_options_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":span_context",
        ":trace",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "span_context_test",
    srcs = ["internal/span_context_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":span_context",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "span_exporter_test",
    srcs = ["internal/span_exporter_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":trace",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "status_test",
    srcs = ["internal/status_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":trace",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "trace_config_test",
    srcs = ["internal/trace_config_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":trace",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "trace_context_test",
    srcs = ["internal/trace_context_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":span_context",
        ":trace_context",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "trace_options_test",
    srcs = ["internal/trace_options_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":span_context",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "with_span_test",
    srcs = ["internal/with_span_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":span_context",
        ":trace",
        ":with_span",
        "//opencensus/context",
        "@com_google_googletest//:gtest_main",
    ],
)

# Benchmarks
# ========================================================================= #

cc_binary(
    name = "attribute_value_ref_benchmark",
    testonly = 1,
    srcs = ["internal/attribute_value_ref_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":trace",
        "@com_github_google_benchmark//:benchmark",
    ],
)

cc_binary(
    name = "b3_benchmark",
    testonly = 1,
    srcs = ["internal/b3_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":b3",
        "@com_github_google_benchmark//:benchmark",
    ],
)

cc_binary(
    name = "cloud_trace_context_benchmark",
    testonly = 1,
    srcs = ["internal/cloud_trace_context_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":cloud_trace_context",
        "@com_github_google_benchmark//:benchmark",
    ],
)

cc_binary(
    name = "grpc_trace_bin_benchmark",
    testonly = 1,
    srcs = ["internal/grpc_trace_bin_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":grpc_trace_bin",
        "@com_github_google_benchmark//:benchmark",
    ],
)

cc_binary(
    name = "sampler_benchmark",
    testonly = 1,
    srcs = ["internal/sampler_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":span_context",
        ":trace",
        "@com_github_google_benchmark//:benchmark",
    ],
)

cc_binary(
    name = "span_benchmark",
    testonly = 1,
    srcs = ["internal/span_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":span_context",
        ":trace",
        "@com_github_google_benchmark//:benchmark",
    ],
)

cc_binary(
    name = "span_id_benchmark",
    testonly = 1,
    srcs = ["internal/span_id_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":span_context",
        "@com_github_google_benchmark//:benchmark",
    ],
)

cc_binary(
    name = "trace_context_benchmark",
    testonly = 1,
    srcs = ["internal/trace_context_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":trace_context",
        "@com_github_google_benchmark//:benchmark",
    ],
)

cc_binary(
    name = "with_span_benchmark",
    testonly = 1,
    srcs = ["internal/with_span_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":trace",
        ":with_span",
        "@com_github_google_benchmark//:benchmark",
    ],
)

# Fuzzers
# ========================================================================= #

cc_fuzz_target(
    name = "b3_fuzzer",
    srcs = ["internal/b3_fuzzer.cc"],
    corpus = glob(["internal/b3_corpus/*"]),
    deps = [
        ":b3",
        "@com_google_absl//absl/strings",
    ],
)

cc_fuzz_target(
    name = "cloud_trace_context_fuzzer",
    srcs = ["internal/cloud_trace_context_fuzzer.cc"],
    corpus = glob(["internal/cloud_trace_context_corpus/*"]),
    deps = [
        ":cloud_trace_context",
        ":span_context",
        "@com_google_absl//absl/strings",
    ],
)

cc_fuzz_target(
    name = "grpc_trace_bin_fuzzer",
    srcs = ["internal/grpc_trace_bin_fuzzer.cc"],
    corpus = glob(["internal/grpc_trace_bin_corpus/*"]),
    deps = [
        ":grpc_trace_bin",
        ":span_context",
        "@com_google_absl//absl/strings",
    ],
)

cc_fuzz_target(
    name = "trace_context_fuzzer",
    srcs = ["internal/trace_context_fuzzer.cc"],
    corpus = glob(["internal/trace_context_corpus/*"]),
    deps = [
        ":span_context",
        ":trace_context",
        "@com_google_absl//absl/strings",
    ],
)
