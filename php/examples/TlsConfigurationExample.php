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

require_once __DIR__ . '/../vendor/autoload.php';
require_once __DIR__ . '/ExampleConfig.php';

use Apache\Rocketmq\TlsCredentials;

echo "========================================\n";
echo "RocketMQ PHP Client - TLS Configuration Examples\n";
echo "========================================\n\n";

// ---------------------------------------------------------------------------
// 1. Insecure (plaintext) — for local development environments only.
//    No encryption, no certificate verification.
// ---------------------------------------------------------------------------
echo "1. Insecure Connection (Plaintext)\n";
echo "   Use case: Local development, testing against a local broker.\n";
echo "   Security: NONE — all data sent in plaintext.\n\n";

$insecure = TlsCredentials::createInsecure();
echo "   isInsecure: " . var_export($insecure->isInsecure(), true) . "\n";
echo "   shouldVerifyPeer: " . var_export($insecure->shouldVerifyPeer(), true) . "\n\n";

// ---------------------------------------------------------------------------
// 2. Default TLS — convenience for development/testing.
//    Uses insecure connection with verification disabled.
// ---------------------------------------------------------------------------
echo "2. Default TLS (Development)\n";
echo "   Use case: Quick-start development without TLS certificates.\n";
echo "   Security: Insecure — peer verification disabled.\n\n";

$default = TlsCredentials::createDefault();
echo "   isInsecure: " . var_export($default->isInsecure(), true) . "\n";
echo "   shouldVerifyPeer: " . var_export($default->shouldVerifyPeer(), true) . "\n";
echo "   shouldVerifyPeerName: " . var_export($default->shouldVerifyPeerName(), true) . "\n\n";

// ---------------------------------------------------------------------------
// 3. CA Certificate — one-way TLS for production.
//    Server presents a certificate verified against a trusted CA.
// ---------------------------------------------------------------------------
echo "3. CA Certificate (One-Way TLS)\n";
echo "   Use case: Production environments with server-side TLS.\n";
echo "   Security: Server identity verified via CA certificate.\n";
echo "   Setup: Place your CA cert at a known path, e.g.:\n";
echo "     export ROCKETMQ_PHP_TLS_CA_CERT=/etc/ssl/ca-cert.pem\n\n";

$caCertPath = getenv('ROCKETMQ_PHP_TLS_CA_CERT') ?: '/path/to/ca-cert.pem';
echo "   Configured CA cert path: {$caCertPath}\n";

if (file_exists($caCertPath)) {
    $withCa = TlsCredentials::createWithCa($caCertPath);
    echo "   CA cert found and loaded.\n";
    echo "   shouldVerifyPeer: " . var_export($withCa->shouldVerifyPeer(), true) . "\n";
    echo "   getCaCertPath: " . $withCa->getCaCertPath() . "\n";
} else {
    echo "   (CA cert file not found — this is a demonstration path)\n";
    echo "   To use: TlsCredentials::createWithCa('/actual/path/to/ca-cert.pem')\n";
}
echo "\n";

// ---------------------------------------------------------------------------
// 4. Mutual TLS (mTLS) — two-way TLS for production.
//    Both client and server present certificates.
// ---------------------------------------------------------------------------
echo "4. Mutual TLS (mTLS)\n";
echo "   Use case: Production environments requiring client authentication.\n";
echo "   Security: Both client and server identities verified.\n";
echo "   Setup: Place your certs at known paths, e.g.:\n";
echo "     export ROCKETMQ_PHP_TLS_CLIENT_CERT=/etc/ssl/client-cert.pem\n";
echo "     export ROCKETMQ_PHP_TLS_CLIENT_KEY=/etc/ssl/client-key.pem\n";
echo "     export ROCKETMQ_PHP_TLS_CA_CERT=/etc/ssl/ca-cert.pem  # optional\n\n";

$clientCertPath = getenv('ROCKETMQ_PHP_TLS_CLIENT_CERT') ?: '/path/to/client-cert.pem';
$clientKeyPath = getenv('ROCKETMQ_PHP_TLS_CLIENT_KEY') ?: '/path/to/client-key.pem';

echo "   Configured client cert path: {$clientCertPath}\n";
echo "   Configured client key path:  {$clientKeyPath}\n";

if (file_exists($clientCertPath) && file_exists($clientKeyPath)) {
    $caForMtls = file_exists($caCertPath) ? $caCertPath : null;
    $mtls = TlsCredentials::createMtls($clientCertPath, $clientKeyPath, $caForMtls);
    echo "   Client cert/key found and loaded.\n";
    echo "   shouldVerifyPeer: " . var_export($mtls->shouldVerifyPeer(), true) . "\n";
    echo "   getClientCertPath: " . $mtls->getClientCertPath() . "\n";
    echo "   getClientKeyPath: " . $mtls->getClientKeyPath() . "\n";
} else {
    echo "   (Client cert/key not found — this is a demonstration path)\n";
    echo "   To use: TlsCredentials::createMtls(\n";
    echo "       '/actual/path/to/client-cert.pem',\n";
    echo "       '/actual/path/to/client-key.pem',\n";
    echo "       '/actual/path/to/ca-cert.pem'  // optional\n";
    echo "   )\n";
}
echo "\n";

// ---------------------------------------------------------------------------
// 5. Using TlsCredentials with a consumer/client
// ---------------------------------------------------------------------------
echo "5. Integration with Consumer/Producer\n";
echo "   Pass TlsCredentials via the 'tlsCredentials' option:\n\n";
echo "   \$tls = TlsCredentials::createWithCa('/etc/ssl/ca-cert.pem');\n";
echo "   \$consumer = new SimpleConsumer(\n";
echo "       'broker:8080',\n";
echo "       'GID_consumer',\n";
echo "       ['tlsCredentials' => \$tls]\n";
echo "   );\n\n";

// Demonstrate toChannelCredentials()
echo "6. Converting to gRPC ChannelCredentials\n";
echo "   The toChannelCredentials() method converts TlsCredentials\n";
echo "   into a Grpc\\ChannelCredentials instance for the gRPC extension.\n\n";

try {
    $channelCreds = $insecure->toChannelCredentials();
    echo "   Insecure → ChannelCredentials: " . ($channelCreds !== null ? get_class($channelCreds) : 'Insecure (null)') . "\n";
} catch (\Exception $e) {
    echo "   Insecure → Error: " . $e->getMessage() . "\n";
}

echo "\n";
echo "========================================\n";
echo "TLS Configuration Examples Complete\n";
echo "========================================\n";
