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

licenses(["notice"])  # Apache License 2.0

package(default_visibility = ["//visibility:public"])

# Libraries
# ========================================================================= #

cc_library(
    name = "zipkin_exporter",
    srcs = [
        "internal/zipkin_exporter.cc",
    ],
    hdrs = [
        "zipkin_exporter.h",
    ],
    copts = DEFAULT_COPTS,
    deps = [
        "//opencensus/trace",
        "@com_github_curl//:curl",
        "@com_github_tencent_rapidjson//:rapidjson",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
    ],
)

# Tests
# ========================================================================= #

cc_test(
    name = "zipkin_exporter_test",
    srcs = ["internal/zipkin_exporter_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":zipkin_exporter",
        "//opencensus/trace",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)
