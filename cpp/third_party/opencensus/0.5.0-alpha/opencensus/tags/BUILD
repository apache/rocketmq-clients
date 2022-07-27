# OpenCensus C++ Tags library.
#
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

load("//opencensus:copts.bzl", "DEFAULT_COPTS", "TEST_COPTS")
load("//bazel:cc_fuzz_target.bzl", "cc_fuzz_target")

licenses(["notice"])  # Apache License 2.0

package(default_visibility = ["//visibility:private"])

# The public tags API.
cc_library(
    name = "tags",
    srcs = [
        "internal/tag_key.cc",
        "internal/tag_map.cc",
    ],
    hdrs = [
        "tag_key.h",
        "tag_map.h",
    ],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/synchronization",
    ],
)

cc_library(
    name = "context_util",
    srcs = ["internal/context_util.cc"],
    hdrs = ["context_util.h"],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":tags",
        "//opencensus/context",
    ],
)

cc_library(
    name = "grpc_tags_bin",
    srcs = ["internal/grpc_tags_bin.cc"],
    hdrs = ["propagation/grpc_tags_bin.h"],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":tags",
        "//opencensus/common/internal:varint",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "with_tag_map",
    srcs = ["internal/with_tag_map.cc"],
    hdrs = ["with_tag_map.h"],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        ":tags",
        "//opencensus/context",
    ],
)

# Tests
# ========================================================================= #

cc_test(
    name = "context_util_test",
    srcs = ["internal/context_util_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":context_util",
        ":tags",
        ":with_tag_map",
        "//opencensus/context",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "grpc_tags_bin_test",
    srcs = ["internal/grpc_tags_bin_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":grpc_tags_bin",
        ":tags",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "tag_key_test",
    srcs = ["internal/tag_key_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":tags",
        "@com_google_absl//absl/hash",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "tag_map_test",
    srcs = ["internal/tag_map_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":tags",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "with_tag_map_test",
    srcs = ["internal/with_tag_map_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":tags",
        ":with_tag_map",
        "//opencensus/context",
        "@com_google_googletest//:gtest_main",
    ],
)

# Benchmarks
# ========================================================================= #

cc_binary(
    name = "grpc_tags_bin_benchmark",
    testonly = 1,
    srcs = ["internal/grpc_tags_bin_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":grpc_tags_bin",
        ":tags",
        "@com_github_google_benchmark//:benchmark",
        "@com_google_absl//absl/strings",
    ],
)

cc_binary(
    name = "tag_map_benchmark",
    testonly = 1,
    srcs = ["internal/tag_map_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":tags",
        "@com_github_google_benchmark//:benchmark",
        "@com_google_absl//absl/strings",
    ],
)

cc_binary(
    name = "with_tag_map_benchmark",
    testonly = 1,
    srcs = ["internal/with_tag_map_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":tags",
        ":with_tag_map",
        "@com_github_google_benchmark//:benchmark",
    ],
)

# Fuzzers
# ========================================================================= #

cc_fuzz_target(
    name = "grpc_tags_bin_fuzzer",
    srcs = ["internal/grpc_tags_bin_fuzzer.cc"],
    corpus = glob(["internal/grpc_tags_bin_corpus/*"]),
    deps = [
        ":grpc_tags_bin",
        "@com_google_absl//absl/strings",
    ],
)
