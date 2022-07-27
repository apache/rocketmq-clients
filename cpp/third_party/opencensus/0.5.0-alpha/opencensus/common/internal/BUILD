# OpenCensus C++ internal libraries.
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

licenses(["notice"])  # Apache 2.0

package(default_visibility = ["//opencensus:__subpackages__"])

cc_library(
    name = "hostname",
    srcs = ["hostname.cc"],
    hdrs = ["hostname.h"],
    copts = DEFAULT_COPTS,
    deps = ["@com_google_absl//absl/strings"],
)

cc_library(
    name = "random_lib",
    srcs = ["random.cc"],
    hdrs = ["random.h"],
    copts = DEFAULT_COPTS,
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/synchronization",
        "@com_google_absl//absl/time",
    ],
)

cc_library(
    name = "stats_object",
    hdrs = ["stats_object.h"],
    copts = DEFAULT_COPTS,
    deps = [
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_absl//absl/types:span",
    ],
)

cc_library(
    name = "string_vector_hash",
    hdrs = ["string_vector_hash.h"],
    copts = DEFAULT_COPTS,
    deps = ["@com_google_absl//absl/hash"],
)

cc_library(
    name = "timestamp",
    srcs = ["timestamp.cc"],
    hdrs = ["timestamp.h"],
    copts = DEFAULT_COPTS,
    deps = [
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_library(
    name = "varint",
    srcs = ["varint.cc"],
    hdrs = ["varint.h"],
    copts = DEFAULT_COPTS,
    deps = ["@com_google_absl//absl/strings"],
)

# Tests
# ========================================================================= #

cc_test(
    name = "hostname_test",
    srcs = ["hostname_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":hostname",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "random_test",
    srcs = ["random_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":random_lib",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "random_benchmark",
    testonly = 1,
    srcs = ["random_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":random_lib",
        "@com_github_google_benchmark//:benchmark",
    ],
)

cc_test(
    name = "stats_object_test",
    srcs = ["stats_object_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":stats_object",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/types:span",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_binary(
    name = "timestamp_benchmark",
    testonly = 1,
    srcs = ["timestamp_benchmark.cc"],
    copts = TEST_COPTS,
    linkstatic = 1,
    deps = [
        ":timestamp",
        "@com_github_google_benchmark//:benchmark",
        "@com_google_absl//absl/time",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "timestamp_test",
    srcs = ["timestamp_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":timestamp",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
        "@com_google_protobuf//:protobuf",
    ],
)

cc_test(
    name = "varint_test",
    srcs = ["varint_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":varint",
        "@com_google_absl//absl/strings",
        "@com_google_googletest//:gtest_main",
    ],
)
