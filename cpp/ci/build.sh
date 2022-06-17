#!/usr/bin/env bash
CI_DIR=$(dirname "$0")
BASE_DIR=$(pwd $CI_DIR/..)

cd $BASE_DIR
mkdir _build
cd _build
cmake ..
make -j $(nproc)
ctest