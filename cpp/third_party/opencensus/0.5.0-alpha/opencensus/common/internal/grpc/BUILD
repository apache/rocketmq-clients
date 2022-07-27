# OpenCensus C++ internal libraries.
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

licenses(["notice"])  # Apache 2.0

package(default_visibility = ["//opencensus:__subpackages__"])

cc_library(
    name = "status",
    srcs = ["status.cc"],
    hdrs = ["status.h"],
    copts = DEFAULT_COPTS,
    deps = [
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/strings",
    ],
)

cc_library(
    name = "with_user_agent",
    srcs = ["with_user_agent.cc"],
    hdrs = ["with_user_agent.h"],
    copts = DEFAULT_COPTS,
    deps = [
        "//opencensus/common:version",
        "@com_github_grpc_grpc//:grpc++",
    ],
)

# Tests
# ========================================================================= #

cc_test(
    name = "status_test",
    srcs = ["status_test.cc"],
    copts = TEST_COPTS,
    deps = [
        ":status",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_googletest//:gtest_main",
    ],
)
