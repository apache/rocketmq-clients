#!/usr/bin/env bash
TOOLS_DIR=$(dirname "$0")
WORKSPACE=$(cd -- "$TOOLS_DIR/.." && pwd)
BUILD_DIR=$WORKSPACE/_build
if [ -d $BUILD_DIR ]; then
    rm -fr $BUILD_DIR
fi

mkdir -p "$BUILD_DIR"

cd $BUILD_DIR
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j $(nproc)
VERSION="rocketmq-client-5.0.0"
DIST_DIR="$WORKSPACE/$VERSION/"
if [ -d "$DIST_DIR" ]; then
    rm -fr $DIST_DIR
fi

mkdir -p "$DIST_DIR/include"
mkdir -p "$DIST_DIR/lib"
mkdir -p "$DIST_DIR/examples"

cp -r $WORKSPACE/include/rocketmq $DIST_DIR/include/
cp $WORKSPACE/examples/*.cpp $DIST_DIR/examples/
cp $WORKSPACE/examples/Makefile $DIST_DIR/examples/

if [[ "$OSTYPE" == "linux-gnu"* ]]; then
  cp $BUILD_DIR/librocketmq.so $DIST_DIR/lib/
elif [[ "$OSTYPE" == "darwin"* ]]; then
  cp $BUILD_DIR/librocketmq.dylib $DIST_DIR/lib/
fi

echo "Create Package"
cd "$WORKSPACE"
tar -czvf "$VERSION.tar.gz" "$VERSION"
