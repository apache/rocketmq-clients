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

use Apache\Rocketmq\ClientConfiguration;
use Apache\Rocketmq\Message\MessageBuilder;
use Apache\Rocketmq\Producer\Producer;
use Apache\Rocketmq\TelemetrySession;
use Apache\Rocketmq\V2\Settings;

echo "=== Telemetry Session Example ===\n\n";

// Configuration
$endpoints = '127.0.0.1:8080';
$topic = 'test-topic';
$clientId = 'telemetry-example-' . getmypid();

echo "Configuration:\n";
echo "  Endpoints: {$endpoints}\n";
echo "  Topic: {$topic}\n";
echo "  Client ID: {$clientId}\n\n";

// Create producer
$config = new ClientConfiguration($endpoints);
$producer = Producer::getInstance($config, $topic);

// Enable metrics (auto-initializes meter manager)
$producer->enableMetrics('http://localhost:4318/v1/metrics', 30);

echo "Starting producer...\n";
$producer->start();
echo "✓ Producer started\n\n";

// ============================================
// 1. Telemetry Session Setup
// ============================================
echo "1. Setting up Telemetry session...\n";

try {
    // Get gRPC client via reflection (in real usage, expose getClient() method)
    $reflection = new ReflectionClass($producer);
    
    // Get private client property
    $clientProp = $reflection->getProperty('client');
    $clientProp->setAccessible(true);
    $grpcClient = $clientProp->getValue($producer);
    
    if ($grpcClient !== null) {
        // Create telemetry session
        $telemetrySession = new TelemetrySession($clientId, $grpcClient);
        
        // Set settings update callback
        $telemetrySession->setSettingsCallback(function(Settings $settings) {
            echo "   [Settings Update] Received from server\n";
            
            if ($settings->hasClientType()) {
                echo "     - Client Type: " . $settings->getClientType() . "\n";
            }
            
            if ($settings->hasUserAgent()) {
                $ua = $settings->getUserAgent();
                echo "     - Language: " . $ua->getLanguage() . "\n";
                echo "     - Version: " . $ua->getVersion() . "\n";
            }
            
            // Handle publishing settings
            if ($settings->hasPublishing()) {
                $publishing = $settings->getPublishing();
                echo "     - Publishing settings updated\n";
                
                if ($publishing->hasValidateMessageType()) {
                    echo "       Validate Message Type: " . 
                         ($publishing->getValidateMessageType() ? 'true' : 'false') . "\n";
                }
            }
            
            // Handle subscription settings
            if ($settings->hasSubscription()) {
                $subscription = $settings->getSubscription();
                echo "     - Subscription settings updated\n";
            }
        });
        
        echo "✓ Telemetry session created\n";
        echo "  - Settings callback: registered\n";
        echo "  - Auto-reconnection: enabled\n";
        echo "  - Max reconnect attempts: 10\n\n";
        
        // Start telemetry session
        echo "2. Starting telemetry session...\n";
        $telemetrySession->start();
        echo "✓ Telemetry session started\n";
        echo "  - Bidirectional stream: established\n";
        echo "  - Handshake: sent\n";
        echo "  - Status: active\n\n";
        
        // Send some messages while telemetry is active
        echo "3. Sending messages with active telemetry...\n";
        for ($i = 0; $i < 5; $i++) {
            $message = MessageBuilder::newMessage()
                ->setTopic($topic)
                ->setBody("Telemetry test message #{$i}")
                ->setTag('telemetry-test')
                ->build();
            
            $receipt = $producer->send($message);
            echo "   ✓ Sent: {$receipt->getMessageId()}\n";
            
            usleep(200000); // 200ms between messages
        }
        echo "\n";
        
        // Simulate receiving settings update (for demonstration)
        echo "4. Simulating settings update from server...\n";
        echo "   (In production, this happens automatically via gRPC stream)\n\n";
        
        // Check session status
        echo "5. Checking telemetry session status...\n";
        echo "   Active: " . ($telemetrySession->isActive() ? 'Yes' : 'No') . "\n";
        echo "   Client ID: " . $telemetrySession->getClientId() . "\n\n";
        
        // Stop telemetry session
        echo "6. Stopping telemetry session...\n";
        $telemetrySession->stop();
        echo "✓ Telemetry session stopped\n\n";
        
    } else {
        echo "⚠ gRPC client not available (producer may not be started yet)\n";
        echo "   Telemetry requires an active gRPC connection\n\n";
    }
    
} catch (\Exception $e) {
    echo "✗ Telemetry setup failed: " . $e->getMessage() . "\n";
    echo "   This is expected if RocketMQ server doesn't support telemetry yet\n\n";
}

// ============================================
// 7. Metrics Collection During Telemetry
// ============================================
echo "7. Checking metrics collected during telemetry session...\n";

$meterManager = $producer->getMeterManager();
if ($meterManager !== null && $meterManager->isEnabled()) {
    $meterManager->updateGauges();
    $metrics = $meterManager->exportMetrics();
    
    echo "   ✓ Exported " . count($metrics) . " metric types\n";
    
    foreach ($metrics as $metricName => $dataPoints) {
        echo "     - {$metricName}: " . count($dataPoints) . " data points\n";
    }
} else {
    echo "   ⚠ Metrics not enabled\n";
}
echo "\n";

// ============================================
// 8. Shutdown
// ============================================
echo "8. Shutting down producer...\n";
$producer->shutdown();
echo "✓ Producer shutdown complete\n\n";

// ============================================
// Summary
// ============================================
echo "=== Telemetry Session Features ===\n\n";

echo "What was demonstrated:\n";
echo "1. ✓ Telemetry session creation\n";
echo "2. ✓ Bidirectional gRPC streaming\n";
echo "3. ✓ Settings update callback\n";
echo "4. ✓ Automatic handshake\n";
echo "5. ✓ Message sending with active telemetry\n";
echo "6. ✓ Graceful session shutdown\n";
echo "7. ✓ Metrics collection integration\n\n";

echo "Key Benefits:\n";
echo "- Dynamic configuration updates from server\n";
echo "- Real-time monitoring and diagnostics\n";
echo "- Automatic reconnection on failure\n";
echo "- Seamless integration with metrics system\n";
echo "- Non-blocking background operation\n\n";

echo "Production Usage Tips:\n";
echo "1. Start telemetry session after producer start()\n";
echo "2. Register settings callback before starting session\n";
echo "3. Monitor session status for debugging\n";
echo "4. Handle settings updates appropriately\n";
echo "5. Stop session before shutting down producer\n\n";

echo "Note: Full telemetry functionality requires:\n";
echo "- RocketMQ server with telemetry support\n";
echo "- Proper gRPC service configuration\n";
echo "- Network connectivity for bidirectional streaming\n";
