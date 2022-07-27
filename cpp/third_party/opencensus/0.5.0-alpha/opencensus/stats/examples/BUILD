# OpenCensus C++ Stats library: Example usage.
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

load("//opencensus:copts.bzl", "TEST_COPTS")

licenses(["notice"])  # Apache 2.0

package(
    default_testonly = 1,
    default_visibility = ["//visibility:private"],
)

# Examples are defined as tests in order to make sure the code compiles and
# doesn't crash.

cc_test(
    name = "view_and_recording_example",
    srcs = ["view_and_recording_example.cc"],
    copts = TEST_COPTS,
    deps = [
        "//opencensus/stats",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)

cc_test(
    name = "exporter_example",
    srcs = ["exporter_example.cc"],
    copts = TEST_COPTS,
    tags = ["noci"],  # This runs slowly and is not very useful as a test.
    deps = [
        "//opencensus/stats",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googletest//:gtest_main",
    ],
)
