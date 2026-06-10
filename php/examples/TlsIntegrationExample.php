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
require_once __DIR__ . '/../TlsCredentials.php';
require_once __DIR__ . '/../ProducerBuilder.php';
require_once __DIR__ . '/../SimpleConsumerBuilder.php';
require_once __DIR__ . '/../PushConsumerBuilder.php';
require_once __DIR__ . '/../Logger.php';
require_once __DIR__ . '/ExampleConfig.php';

use Apache\Rocketmq\TlsCredentials;
use Apache\Rocketmq\ProducerBuilder;
use Apache\Rocketmq\SimpleConsumerBuilder;
use Apache\Rocketmq\PushConsumerBuilder;
use Apache\Rocketmq\ClientConfigurationBuilder;

echo "========================================\n";
echo "RocketMQ PHP Client - TLS Integration Examples\n";
echo "========================================\n\n";

// Load configuration
$config = ExampleConfig::getInstance();
$endpoints = $config->getEndpoints();
$topic = $config->getTopic('normal');
$consumerGroup = $config->getConsumerGroup();
$credentials = $config->getCredentials();
$sslEnabled = $config->isSslEnabled();

$config->display();

// ---------------------------------------------------------------------------
// 1. Producer with TLS — CA Certificate (One-Way TLS)
// ---------------------------------------------------------------------------
echo "1. Producer with One-Way TLS (CA Certificate)\n\n";

$caCertPath = getenv('ROCKETMQ_PHP_TLS_CA_CERT') ?: '/path/to/ca-cert.pem';

if (file_exists($caCertPath)) {
    $tls = TlsCredentials::createWithCa($caCertPath);

    $producer = (new ProducerBuilder())
        ->setEndpoints($endpoints)
        ->setTopics($topic)
        ->setTlsCredentials($tls)
        ->setMaxAttempts(3)
        ->build();

    echo "   Producer started with CA certificate TLS.\n";
    echo "   TLS verifyPeer: " . var_export($tls->shouldVerifyPeer(), true) . "\n\n";

    $producer->shutdown();
} else {
    echo "   Skipped — CA cert not found at: {$caCertPath}\n";
    echo "   Usage: export ROCKETMQ_PHP_TLS_CA_CERT=/etc/ssl/ca-cert.pem\n\n";
}

// ---------------------------------------------------------------------------
// 2. Producer with mTLS (Mutual TLS)
// ---------------------------------------------------------------------------
echo "2. Producer with Mutual TLS (mTLS)\n\n";

$clientCertPath = getenv('ROCKETMQ_PHP_TLS_CLIENT_CERT') ?: '/path/to/client-cert.pem';
$clientKeyPath = getenv('ROCKETMQ_PHP_TLS_CLIENT_KEY') ?: '/path/to/client-key.pem';

if (file_exists($clientCertPath) && file_exists($clientKeyPath)) {
    $caForMtls = file_exists($caCertPath) ? $caCertPath : null;
    $mtls = TlsCredentials::createMtls($clientCertPath, $clientKeyPath, $caForMtls);

    $producer = (new ProducerBuilder())
        ->setEndpoints($endpoints)
        ->setTopics($topic)
        ->setTlsCredentials($mtls)
        ->build();

    echo "   Producer started with mutual TLS.\n";
    echo "   Client cert: " . $mtls->getClientCertPath() . "\n\n";

    $producer->shutdown();
} else {
    echo "   Skipped — client cert/key not found.\n";
    echo "   Usage:\n";
    echo "     export ROCKETMQ_PHP_TLS_CLIENT_CERT=/etc/ssl/client-cert.pem\n";
    echo "     export ROCKETMQ_PHP_TLS_CLIENT_KEY=/etc/ssl/client-key.pem\n\n";
}

// ---------------------------------------------------------------------------
// 3. SimpleConsumer with TLS via ClientConfigurationBuilder
// ---------------------------------------------------------------------------
echo "3. SimpleConsumer with TLS via ClientConfigurationBuilder\n\n";

$ccBuilder = (new ClientConfigurationBuilder())
    ->setEndpoints($endpoints);

if ($credentials !== null) {
    $ccBuilder->setCredentialProvider($credentials);
}

// Enable TLS with CA certificate
if (file_exists($caCertPath)) {
    $tls = TlsCredentials::createWithCa($caCertPath);
    $ccBuilder->setTlsCredentials($tls)->enableSsl(true);
    echo "   TLS enabled with CA certificate.\n";
} else {
    $ccBuilder->enableSsl($sslEnabled);
    echo "   TLS skipped, using default sslEnabled={$sslEnabled}.\n";
}

$clientConfig = $ccBuilder->build();

try {
    $consumer = (new SimpleConsumerBuilder())
        ->setClientConfiguration($clientConfig)
        ->setConsumerGroup($consumerGroup)
        ->setAwaitDuration(30)
        ->build();

    echo "   SimpleConsumer started with TLS configuration.\n";
    $consumer->shutdown();
} catch (\Throwable $e) {
    echo "   SimpleConsumer start failed: " . $e->getMessage() . "\n";
}
echo "\n";

// ---------------------------------------------------------------------------
// 4. PushConsumer with Insecure Connection (local development)
// ---------------------------------------------------------------------------
echo "4. PushConsumer with Insecure Connection (local dev)\n\n";

$insecure = TlsCredentials::createInsecure();

$ccBuilder2 = (new ClientConfigurationBuilder())
    ->setEndpoints($endpoints)
    ->setTlsCredentials($insecure)
    ->enableSsl(true);

if ($credentials !== null) {
    $ccBuilder2->setCredentialProvider($credentials);
}

$clientConfig2 = $ccBuilder2->build();

try {
    $consumer = (new PushConsumerBuilder())
        ->setClientConfiguration($clientConfig2)
        ->setConsumerGroup($consumerGroup)
        ->setSubscriptionExpressions([$topic => '*'])
        ->setMessageListener(function ($message) {
            echo "   Received: " . $message->getBody() . "\n";
        })
        ->build();

    echo "   PushConsumer started with insecure TLS (plaintext).\n";
    $consumer->shutdown();
} catch (\Throwable $e) {
    echo "   PushConsumer start failed: " . $e->getMessage() . "\n";
}
echo "\n";

// ---------------------------------------------------------------------------
// 5. Direct instantiation with tlsCredentials option
// ---------------------------------------------------------------------------
echo "5. Direct Instantiation with tlsCredentials Option\n\n";
echo "   \$tls = TlsCredentials::createWithCa('/etc/ssl/ca-cert.pem');\n";
echo "   \$producer = new Producer(\$endpoints, [\n";
echo "       'topics' => ['\${topic}'],\n";
echo "       'tlsCredentials' => \$tls,\n";
echo "       'sslEnabled' => true,\n";
echo "       'credentials' => \$credentials,\n";
echo "   ]);\n";
echo "   \$producer->start();\n\n";

echo "========================================\n";
echo "TLS Integration Examples Complete\n";
echo "========================================\n";
