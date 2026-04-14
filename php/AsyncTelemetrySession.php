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

namespace Apache\Rocketmq;

use Apache\Rocketmq\V2\TelemetryCommand;
use Grpc\BaseStub;

/**
 * AsyncTelemetrySession using Swoole coroutines
 * 
 * This implementation uses Swoole's coroutine support to handle
 * bidirectional gRPC streams asynchronously, similar to Java and Node.js.
 * 
 * Requirements:
 * - Swoole extension installed and enabled
 * - PHP >= 7.2
 * 
 * Usage:
 * ```php
 * $session = new AsyncTelemetrySession($client, $clientId, ...);
 * $session->start();
 * 
 * // In a Swoole server or coroutine context:
 * go(function() use ($session) {
 *     $session->run();
 * });
 * ```
 */
class AsyncTelemetrySession
{
    private $client;
    private $clientId;
    private $consumerGroup;
    private $topic;
    private $clientType;
    private $longPollingTimeout;
    
    private $streamCall = null;
    private $isActive = false;
    private $running = false;
    
    /**
     * @var int Reconnect backoff delay in seconds
     */
    const RECONNECT_DELAY = 1;
    
    /**
     * Constructor
     * 
     * @param BaseStub $client gRPC client
     * @param string $clientId Client ID
     * @param string $consumerGroup Consumer group name
     * @param string $topic Topic name
     * @param int $clientType Client type constant
     * @param int $longPollingTimeout Long polling timeout in seconds
     */
    public function __construct(
        BaseStub $client,
        string $clientId,
        string $consumerGroup,
        string $topic,
        int $clientType,
        int $longPollingTimeout = 30
    ) {
        if (!extension_loaded('swoole')) {
            throw new \RuntimeException(
                "Swoole extension is required for AsyncTelemetrySession. " .
                "Please install it: pecl install swoole"
            );
        }
        
        $this->client = $client;
        $this->clientId = $clientId;
        $this->consumerGroup = $consumerGroup;
        $this->topic = $topic;
        $this->clientType = $clientType;
        $this->longPollingTimeout = $longPollingTimeout;
    }
    
    /**
     * Start the telemetry session
     * 
     * @return void
     * @throws \Exception If Swoole is not available
     */
    public function start(): void
    {
        if (!defined('SWOOLE_VERSION')) {
            throw new \Exception(
                "AsyncTelemetrySession requires Swoole extension. " .
                "Install it with: pecl install swoole"
            );
        }
        
        $this->running = true;
        
        // Create a coroutine to run the session
        go(function() {
            $this->run();
        });
        
        Logger::info("AsyncTelemetrySession started in coroutine, clientId={$this->clientId}");
    }
    
    /**
     * Run the telemetry session loop (executed in coroutine)
     * 
     * @return void
     */
    private function run(): void
    {
        Logger::info("Telemetry session coroutine started, clientId={$this->clientId}");
        
        while ($this->running) {
            try {
                // Create stream and send settings
                $this->createStreamAndSync();
                
                // Keep the session alive
                $this->keepAlive();
                
            } catch (\Exception $e) {
                Logger::error(
                    "Telemetry session error, clientId={$this->clientId}, " .
                    "error: " . $e->getMessage()
                );
                
                // Mark as inactive
                $this->isActive = false;
                $this->streamCall = null;
                
                // Wait before reconnect
                if ($this->running) {
                    \Swoole\Coroutine::sleep(self::RECONNECT_DELAY);
                }
            }
        }
        
        Logger::info("Telemetry session coroutine stopped, clientId={$this->clientId}");
    }
    
    /**
     * Create stream and sync settings
     * 
     * @return void
     * @throws \Exception If stream creation fails
     */
    private function createStreamAndSync(): void
    {
        // Create bidirectional stream
        $this->streamCall = $this->client->Telemetry();
        
        if (!$this->streamCall) {
            throw new \Exception("Failed to create telemetry stream");
        }
        
        $this->isActive = true;
        Logger::info("Telemetry stream created, clientId={$this->clientId}");
        
        // Send initial settings
        $this->sendSettings();
        
        // Start a separate coroutine to read responses
        go(function() {
            $this->readResponses();
        });
    }
    
    /**
     * Send settings to the server
     * 
     * @return void
     * @throws \Exception If sending fails
     */
    private function sendSettings(): void
    {
        if (!$this->isActive || !$this->streamCall) {
            throw new \Exception("Telemetry session is not active");
        }
        
        $settings = $this->buildSettings();
        $command = new TelemetryCommand();
        $command->setSettings($settings);
        
        // Write to stream
        $this->streamCall->write($command);
        
        Logger::info("Settings sent via async stream, clientId={$this->clientId}");
    }
    
    /**
     * Build Settings object
     * 
     * @return \Apache\Rocketmq\V2\Settings
     */
    private function buildSettings(): \Apache\Rocketmq\V2\Settings
    {
        $settings = new \Apache\Rocketmq\V2\Settings();
        $settings->setClientType($this->clientType);
        
        // Build subscription
        $subscription = new \Apache\Rocketmq\V2\Subscription();
        
        $group = new \Apache\Rocketmq\V2\Resource();
        $group->setName($this->consumerGroup);
        $subscription->setGroup($group);
        
        $longPollingDuration = new \Google\Protobuf\Duration();
        $longPollingDuration->setSeconds($this->longPollingTimeout);
        $subscription->setLongPollingTimeout($longPollingDuration);
        
        // Add subscription entry for the topic
        $entry = new \Apache\Rocketmq\V2\SubscriptionEntry();
        $topicResource = new \Apache\Rocketmq\V2\Resource();
        $topicResource->setName($this->topic);
        $entry->setTopic($topicResource);
        
        $subscription->setSubscriptions([$entry]);
        $settings->setSubscription($subscription);
        
        return $settings;
    }
    
    /**
     * Read responses from the stream (runs in separate coroutine)
     * 
     * @return void
     */
    private function readResponses(): void
    {
        Logger::info("Response reader coroutine started, clientId={$this->clientId}");
        
        try {
            while ($this->isActive && $this->streamCall) {
                // Note: PHP gRPC extension doesn't support non-blocking reads in coroutines
                // This is a simplified implementation that keeps the connection alive
                // In a production environment, you would need to implement proper async reading
                
                // For now, we just keep the coroutine alive and log status
                \Swoole\Coroutine::sleep(5);
                
                // Periodically check if stream is still valid
                if (!$this->streamCall) {
                    Logger::warn("Stream closed unexpectedly, clientId={$this->clientId}");
                    break;
                }
            }
        } catch (\Exception $e) {
            Logger::error("Response reader error, clientId={$this->clientId}: " . $e->getMessage());
        }
        
        Logger::info("Response reader coroutine stopped, clientId={$this->clientId}");
    }
    
    /**
     * Keep the session alive
     * This method blocks until the session is stopped or an error occurs
     * 
     * @return void
     */
    private function keepAlive(): void
    {
        // In a real implementation, this would wait for the stream to close
        // For now, we just sleep and check periodically
        while ($this->isActive && $this->running) {
            \Swoole\Coroutine::sleep(5);
        }
    }
    
    /**
     * Stop the telemetry session
     * 
     * @return void
     */
    public function stop(): void
    {
        $this->running = false;
        $this->isActive = false;
        
        if ($this->streamCall) {
            try {
                $this->streamCall->writesDone();
            } catch (\Exception $e) {
                Logger::error("Error closing stream: " . $e->getMessage());
            }
        }
        
        Logger::info("AsyncTelemetrySession stopped, clientId={$this->clientId}");
    }
    
    /**
     * Check if the session is active
     * 
     * @return bool
     */
    public function isActive(): bool
    {
        return $this->isActive;
    }
}
