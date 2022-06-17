#!/usr/bin/env bash
TOOLS_DIR=$(dirname "$0")
WORKSPACE_DIR=$(dirname "$TOOLS_DIR")
cd $WORKSPACE_DIR

find . -iname *.h -o -iname *.cpp | xargs clang-format -i