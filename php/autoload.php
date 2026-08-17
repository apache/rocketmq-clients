<?php
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Simple PSR-4 autoloader for RocketMQ SDK.
 * Handles V2 gRPC classes, GPBMetadata, and Google\Protobuf well-known types.
 */

// Suppress gRPC C-core stderr noise on Windows CI.
// GRPC_VERBOSITY/GRPC_TRACE are read at OS level by the gRPC C library,
// and phpunit.xml <env> tags only set PHP $_ENV, not the process environment.
// Setting them via putenv ensures the C library can read them.
if (!getenv('GRPC_VERBOSITY')) {
    putenv('GRPC_VERBOSITY=ERROR');
    $_ENV['GRPC_VERBOSITY'] = 'ERROR';
}
if (!getenv('GRPC_TRACE')) {
    putenv('GRPC_TRACE=none');
    $_ENV['GRPC_TRACE'] = 'none';
}

// Also attempt ini-level suppression if the gRPC extension supports it.
@ini_set('grpc.grpc_verbosity', 'ERROR');
@ini_set('grpc.grpc_trace', 'none');

spl_autoload_register(function (string $class): void {
    $baseDir = __DIR__;
    $grpcDir = __DIR__ . '/grpc/';

    // Apache\Rocketmq\* -> current directory (excluding V2 which is in grpc/)
    $prefix = 'Apache\\Rocketmq\\';
    $prefixLen = strlen($prefix);
    if (strncmp($class, $prefix, $prefixLen) === 0) {
        $relativeClass = substr($class, $prefixLen);
        // Skip V2 classes - they are handled separately below
        if (strpos($relativeClass, 'V2\\') !== 0) {
            $file = $baseDir . '/' . str_replace('\\', '/', $relativeClass) . '.php';
            if (is_file($file)) {
                require_once $file;
                return;
            }
        }
    }

    // Apache\Rocketmq\V2\* -> grpc/Apache/Rocketmq/V2/
    $prefix = 'Apache\\Rocketmq\\V2\\';
    $prefixLen = strlen($prefix);
    if (strncmp($class, $prefix, $prefixLen) === 0) {
        $relativeClass = str_replace('\\', '/', substr($class, $prefixLen));
        $file = $grpcDir . 'Apache/Rocketmq/V2/' . $relativeClass . '.php';
        if (is_file($file)) {
            require_once $file;
            return;
        }
    }

    // GPBMetadata\* -> grpc/GPBMetadata/
    $prefix2 = 'GPBMetadata\\';
    $prefixLen2 = strlen($prefix2);
    if (strncmp($class, $prefix2, $prefixLen2) === 0) {
        $relativeClass = str_replace('\\', '/', substr($class, $prefixLen2));
        $file = $grpcDir . 'GPBMetadata/' . $relativeClass . '.php';
        if (is_file($file)) {
            require_once $file;
            return;
        }
    }

    // Google\Protobuf\* -> vendor/google/protobuf/src/Google/Protobuf/
    // (well-known types like Timestamp, Duration)
    $prefix3 = 'Google\\Protobuf\\';
    $prefixLen3 = strlen($prefix3);
    if (strncmp($class, $prefix3, $prefixLen3) === 0) {
        $relativeClass = str_replace('\\', '/', substr($class, $prefixLen3));
        $file = __DIR__ . '/vendor/google/protobuf/src/Google/Protobuf/' . $relativeClass . '.php';
        if (is_file($file)) {
            require_once $file;
            return;
        }
    }

    // GPBMetadata\Google\Protobuf\* -> vendor/google/protobuf/src/GPBMetadata/Google/Protobuf/
    $prefix4 = 'GPBMetadata\\Google\\Protobuf\\';
    $prefixLen4 = strlen($prefix4);
    if (strncmp($class, $prefix4, $prefixLen4) === 0) {
        $relativeClass = str_replace('\\', '/', substr($class, $prefixLen4));
        $file = __DIR__ . '/vendor/google/protobuf/src/GPBMetadata/Google/Protobuf/' . $relativeClass . '.php';
        if (is_file($file)) {
            require_once $file;
            return;
        }
    }

    // Grpc\* -> vendor/grpc/grpc/src/lib/
    if (strpos($class, 'Grpc\\') === 0) {
        $relativeClass = str_replace('\\', '/', substr($class, 5));
        $file = __DIR__ . '/vendor/grpc/grpc/src/lib/' . $relativeClass . '.php';
        if (is_file($file)) {
            require_once $file;
            return;
        }
    }
});
