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

load("//opencensus:copts.bzl", "DEFAULT_COPTS")

licenses(["notice"])  # Apache License 2.0

package(default_visibility = ["//visibility:private"])

# Libraries
# ========================================================================= #

cc_library(
    name = "stackdriver_exporter",
    srcs = ["internal/stackdriver_exporter.cc"],
    hdrs = ["stackdriver_exporter.h"],
    copts = DEFAULT_COPTS,
    visibility = ["//visibility:public"],
    deps = [
        "//opencensus/common:version",
        "//opencensus/common/internal:timestamp",
        "//opencensus/common/internal/grpc:status",
        "//opencensus/common/internal/grpc:with_user_agent",
        "//opencensus/trace",
        "@com_github_grpc_grpc//:grpc++",
        "@com_google_absl//absl/base:core_headers",
        "@com_google_absl//absl/memory",
        "@com_google_absl//absl/strings",
        "@com_google_absl//absl/time",
        "@com_google_googleapis//google/devtools/cloudtrace/v2:cloudtrace_cc_grpc",
        "@com_google_googleapis//google/devtools/cloudtrace/v2:cloudtrace_cc_proto",
    ],
)
