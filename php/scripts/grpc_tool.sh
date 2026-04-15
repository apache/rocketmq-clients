#!/bin/bash
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

set -e

# Color definitions
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Path configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
PROTO_ROOT="${PROJECT_ROOT}/protocol"
PHP_GRPC_OUTPUT="${PROJECT_ROOT}/grpc"

# Proto file list
PROTO_FILES=(
    "apache/rocketmq/v2/admin.proto"
    "apache/rocketmq/v2/definition.proto"
    "apache/rocketmq/v2/service.proto"
)

# Display help information
show_help() {
    echo "========================================="
    echo "RocketMQ PHP gRPC Tool"
    echo "========================================="
    echo ""
    echo "Usage: $0 [command]"
    echo ""
    echo "Commands:"
    echo "  install    Install grpc_php_plugin"
    echo "  generate   Generate PHP gRPC code from proto files"
    echo "  all        Install plugin and generate code (default)"
    echo "  help       Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 install    # Only install grpc_php_plugin"
    echo "  $0 generate   # Only generate code (requires plugin installed)"
    echo "  $0 all        # Install plugin and generate code"
    echo "  $0            # Same as 'all'"
    echo ""
}

# Check if protoc is installed
check_protoc() {
    if ! command -v protoc &> /dev/null; then
        echo -e "${RED}Error: protoc compiler not found${NC}"
        echo "Please install protobuf compiler:"
        echo "  macOS: brew install protobuf"
        echo "  Ubuntu: apt-get install protobuf-compiler"
        echo "  CentOS: yum install protobuf-devel"
        exit 1
    fi
    
    PROTOC_VERSION=$(protoc --version)
    echo -e "${GREEN}✓ protoc installed: ${PROTOC_VERSION}${NC}"
}

# Install grpc_php_plugin
install_grpc_plugin() {
    echo ""
    echo "========================================="
    echo "Installing grpc_php_plugin"
    echo "========================================="
    echo ""
    
    # Check if already installed
    if command -v grpc_php_plugin &> /dev/null; then
        echo -e "${GREEN}✓ grpc_php_plugin is already installed${NC}"
        grpc_php_plugin --version
        GRPC_PLUGIN_PATH=$(which grpc_php_plugin)
        return 0
    fi
    
    # Check vendor/bin directory
    if [ -f "${SCRIPT_DIR}/../vendor/bin/grpc_php_plugin" ]; then
        echo -e "${GREEN}✓ grpc_php_plugin found in Composer${NC}"
        GRPC_PLUGIN_PATH="${SCRIPT_DIR}/../vendor/bin/grpc_php_plugin"
        GRPC_PLUGIN_VERSION=$($GRPC_PLUGIN_PATH --version 2>&1 || true)
        echo "Version: ${GRPC_PLUGIN_VERSION}"
        return 0
    fi
    
    echo -e "${YELLOW}! grpc_php_plugin not found, starting installation...${NC}"
    echo ""
    
    # Check git
    if ! command -v git &> /dev/null; then
        echo -e "${RED}Error: git is required${NC}"
        exit 1
    fi
    
    # Check cmake
    if ! command -v cmake &> /dev/null; then
        echo -e "${RED}Error: cmake is required${NC}"
        echo "Please run: brew install cmake"
        exit 1
    fi
    
    # Check make
    if ! command -v make &> /dev/null; then
        echo -e "${RED}Error: make is required (Xcode Command Line Tools)${NC}"
        echo "Please run: xcode-select --install"
        exit 1
    fi
    
    echo -e "${GREEN}✓ All prerequisites are ready${NC}"
    echo ""
    
    # If grpc directory exists, use it directly
    if [ -d "/tmp/grpc" ]; then
        echo "Using existing grpc repository..."
        cd /tmp/grpc
        
        # Ensure submodules are initialized
        if [ ! -d "third_party/protobuf" ]; then
            echo "Initializing submodules..."
            git submodule update --init --recursive
        fi
    else
        echo "Cloning grpc repository..."
        cd /tmp
        git clone --recursive https://github.com/grpc/grpc.git
        cd grpc
    fi
    
    echo ""
    echo "Creating build directory..."
    mkdir -p cmake/build
    cd cmake/build
    
    echo ""
    echo "Configuring CMake..."
    cmake -DgRPC_INSTALL=ON \
          -DgRPC_BUILD_CODEGEN=ON \
          -DCMAKE_BUILD_TYPE=Release \
          ../..
    
    echo ""
    echo "Compiling grpc_php_plugin (this may take a few minutes)..."
    make grpc_php_plugin -j$(sysctl -n hw.ncpu)
    
    echo ""
    echo "Installing grpc_php_plugin..."
    sudo make install
    
    echo ""
    echo "Verifying installation..."
    if command -v grpc_php_plugin &> /dev/null; then
        echo -e "${GREEN}✓ Installation successful!${NC}"
        grpc_php_plugin --version
        echo ""
        echo "Plugin location: $(which grpc_php_plugin)"
        GRPC_PLUGIN_PATH=$(which grpc_php_plugin)
    else
        echo -e "${RED}✗ Installation failed${NC}"
        exit 1
    fi
}

# Check if grpc_php_plugin is installed
check_grpc_php_plugin() {
    # First check system PATH
    if command -v grpc_php_plugin &> /dev/null; then
        GRPC_PLUGIN_PATH=$(which grpc_php_plugin)
        echo -e "${GREEN}✓ grpc_php_plugin installed (system): ${GRPC_PLUGIN_PATH}${NC}"
        return 0
    fi
    
    # Check /tmp/grpc/install/bin (compiled from source)
    if [ -f "/tmp/grpc/install/bin/grpc_php_plugin" ]; then
        GRPC_PLUGIN_PATH="/tmp/grpc/install/bin/grpc_php_plugin"
        echo -e "${GREEN}✓ grpc_php_plugin installed (local build): ${GRPC_PLUGIN_PATH}${NC}"
        return 0
    fi
    
    # Check vendor/bin directory
    if [ -f "${SCRIPT_DIR}/../vendor/bin/grpc_php_plugin" ]; then
        GRPC_PLUGIN_PATH="${SCRIPT_DIR}/../vendor/bin/grpc_php_plugin"
        echo -e "${GREEN}✓ grpc_php_plugin installed (Composer): ${GRPC_PLUGIN_PATH}${NC}"
        return 0
    fi
    
    echo -e "${RED}Error: grpc_php_plugin not found${NC}"
    echo ""
    echo "Please install grpc_php_plugin using one of the following methods:"
    echo ""
    echo "Method 1: Use this script (recommended)"
    echo "  $0 install"
    echo ""
    echo "Method 2: Compile from source manually"
    echo "  git clone --recursive https://github.com/grpc/grpc.git"
    echo "  cd grpc"
    echo "  mkdir cmake/build && cd cmake/build"
    echo "  cmake -DgRPC_INSTALL=ON -DgRPC_BUILD_CODEGEN=ON ../.."
    echo "  make grpc_php_plugin"
    echo "  sudo make install"
    echo ""
    echo "Method 3: Use PECL (may not include plugin)"
    echo "  pecl install grpc"
    echo ""
    echo "Note: Composer's grpc/grpc package only provides PHP runtime, not code generation plugin."
    echo ""
    exit 1
}

# Clean old generated files
cleanup_old_files() {
    echo ""
    echo "Cleaning old generated files..."
    
    if [ -d "$PHP_GRPC_OUTPUT" ]; then
        rm -rf "${PHP_GRPC_OUTPUT}/Apache"
        rm -rf "${PHP_GRPC_OUTPUT}/GPBMetadata"
        echo -e "${GREEN}✓ Old files cleaned${NC}"
    else
        mkdir -p "$PHP_GRPC_OUTPUT"
        echo -e "${YELLOW}! Created output directory: ${PHP_GRPC_OUTPUT}${NC}"
    fi
}

# Generate PHP gRPC code
generate_php_grpc() {
    local proto_path=$1
    local proto_file=$2
    local output_dir=$3
    
    echo ""
    echo "Generating ${proto_file} ..."
    
    protoc \
        --proto_path="${proto_path}" \
        --php_out="${output_dir}" \
        --grpc_out="${output_dir}" \
        --plugin=protoc-gen-grpc="${GRPC_PLUGIN_PATH}" \
        "${proto_path}/${proto_file}"
    
    echo -e "${GREEN}✓ Generation completed${NC}"
}

# Generate code main function
generate_code() {
    echo "========================================="
    echo "RocketMQ PHP gRPC Code Generation"
    echo "========================================="
    echo ""
    
    echo "Checking dependencies..."
    check_protoc
    check_grpc_php_plugin
    
    cleanup_old_files
    
    echo ""
    echo "Starting PHP gRPC code generation..."
    echo "Proto source directory: ${PROTO_ROOT}"
    echo "Output directory: ${PHP_GRPC_OUTPUT}"
    echo ""
    
    # Generate each proto file
    for proto_file in "${PROTO_FILES[@]}"; do
        generate_php_grpc "${PROTO_ROOT}" "${proto_file}" "${PHP_GRPC_OUTPUT}"
    done
    
    echo ""
    echo "========================================="
    echo -e "${GREEN}✓ All files generated successfully!${NC}"
    echo "========================================="
    echo ""
    echo "Generated files location: ${PHP_GRPC_OUTPUT}"
    echo ""
    echo "Next steps:"
    echo "1. Check if generated code is correct"
    echo "2. Run composer dump-autoload to update autoloading"
    echo "3. Run tests to verify functionality"
    echo ""
}

# Main function
main() {
    local command=${1:-all}
    
    case $command in
        install)
            check_protoc
            install_grpc_plugin
            ;;
        generate)
            generate_code
            ;;
        all)
            echo "========================================="
            echo "RocketMQ PHP gRPC Tool"
            echo "========================================="
            echo ""
            check_protoc
            install_grpc_plugin
            generate_code
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            echo -e "${RED}Error: Unknown command '${command}'${NC}"
            echo ""
            show_help
            exit 1
            ;;
    esac
}

# Execute main function
main "$@"
