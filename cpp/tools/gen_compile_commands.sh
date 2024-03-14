TOOLS_DIR=$(dirname "$0")
WORKSPACE_DIR=$(dirname "$TOOLS_DIR")
cd $WORKSPACE_DIR

bazel run @hedron_compile_commands//:refresh_all