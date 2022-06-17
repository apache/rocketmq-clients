#!/usr/bin/env bash
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