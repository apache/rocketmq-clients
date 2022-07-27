#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

TOOLS_DIR=$(dirname "$0")
WORKSPACE_DIR=$(dirname "$TOOLS_DIR")
cd $WORKSPACE_DIR

if test "$#" -lt 1; then
  echo "Use bazel cache for Development Environment"
  bazel coverage --config=remote_cache //src/test/cpp/ut/...
elif test "$1" = "ci"; then
  echo "Use bazel cache for CI"
  bazel coverage --config=ci_remote_cache //src/test/cpp/ut/...
else
  echo "Unknown argument $*. Use bazel cache for Development Environment"
  bazel coverage --config=remote_cache //src/test/cpp/ut/...
fi

genhtml bazel-out/_coverage/_coverage_report.dat --output-directory coverage