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
    private $namespace;
    private $clientType;
    private $longPollingTimeout;
    
    /** @var array<string, \Apache\Rocketmq\Consumer\FilterExpression> Subscription expressions */
    private $subscriptionExpressions = [];
    
    private $streamCall = null;
    private $isActive = false;
    private $running = false;
    
    /**
     * @var bool Whether first settings response has been received
     */
    private $firstSettingsReceived = false;
    
    /**
     * @var \Swoole\Coroutine\Channel|null Channel for startup synchronization
     */
    private $startupChannel = null;
    
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
     * @param string $topic Topic name (for backward compatibility)
     * @param int $clientType Client type constant
     * @param int $longPollingTimeout Long polling timeout in seconds
     * @param string $namespace Namespace (optional, defaults to empty)
     */
    public function __construct(
        BaseStub $client,
        string $clientId,
        string $consumerGroup,
        string $topic,
        int $clientType,
        int $longPollingTimeout = 30,
        string $namespace = ''
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
        $this->namespace = $namespace;
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
        Logger::debug("Creating telemetry stream..., clientId={}", [$this->clientId]);
        // Create bidirectional stream
        $this->streamCall = $this->client->Telemetry();
        Logger::debug("Telemetry stream created, streamCall={}, clientId={}", [
            $this->streamCall ? 'success' : 'null',
            $this->clientId
        ]);
        
        if (!$this->streamCall) {
            throw new \Exception("Failed to create telemetry stream");
        }
        
        $this->isActive = true;
        Logger::info("Telemetry stream created, clientId={$this->clientId}");
        
        // Send initial settings
        Logger::debug("Sending initial settings..., clientId={}", [$this->clientId]);
        $this->sendSettings();
        Logger::debug("Initial settings sent, clientId={}", [$this->clientId]);
        
        // Start a separate coroutine to read responses
        Logger::debug("Starting response reader coroutine..., clientId={}", [$this->clientId]);
        go(function() {
            $this->readResponses();
        });
        Logger::debug("Response reader coroutine started, clientId={}", [$this->clientId]);
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

        $subscriptionExpressions = property_exists($this, 'currentSubscriptionExpressions') ?
            $this->currentSubscriptionExpressions : [];
        $settings = $this->buildSettings($subscriptionExpressions);
        
        $command = new TelemetryCommand();
        $command->setSettings($settings);
        
        // Write to stream
        $this->streamCall->write($command);
        
        Logger::info("Settings sent via async stream, clientId={$this->clientId}");
    }

    public function sendCustomSettings(\Apache\Rocketmq\V2\Settings $settings): void
    {
        if (!$this->isActive() || !$this->streamCall) {
            Logger::warn("Cannot send custom settings, session is not active or stream is closed, clientId={}", [
                $this->clientId
            ]);
            return;
        }
        $command = new TelemetryCommand();
        $command->setSettings($settings);
        $this->streamCall->write($command);
        Logger::info("Custom settings sent via async stream, clientId={$this->clientId}");
    }
    
    /**
     * Build Settings object (aligned with Java SimpleSubscriptionSettings.toProtobuf())
     * 
     * @param array $subscriptionExpressions Topic -> FilterExpression map
     * @return \Apache\Rocketmq\V2\Settings
     */
    private function buildSettings(array $subscriptionExpressions = []): \Apache\Rocketmq\V2\Settings
    {
        $settings = new \Apache\Rocketmq\V2\Settings();
        $settings->setClientType($this->clientType);
        
        // Set user agent
        $ua = new \Apache\Rocketmq\V2\UA();
        $ua->setLanguage(\Apache\Rocketmq\V2\Language::PHP);
        $ua->setVersion(\Apache\Rocketmq\Util::getSdkVersion());
        $settings->setUserAgent($ua);
        
        // Build subscription
        $subscription = new \Apache\Rocketmq\V2\Subscription();
        
        // Set group with namespace ✅
        $group = new \Apache\Rocketmq\V2\Resource();
        $group->setResourceNamespace($this->namespace);  // 设置 namespace
        $group->setName($this->consumerGroup);
        $subscription->setGroup($group);
        
        // Set long polling timeout
        $longPollingDuration = new \Google\Protobuf\Duration();
        $longPollingDuration->setSeconds($this->longPollingTimeout);
        $subscription->setLongPollingTimeout($longPollingDuration);
        
        // Build subscription entries from subscription expressions ✅
        $subscriptionEntries = [];
        if (!empty($subscriptionExpressions)) {
            foreach ($subscriptionExpressions as $topic => $filterExpression) {
                // Create topic resource with namespace ✅
                $topicResource = new \Apache\Rocketmq\V2\Resource();
                $topicResource->setResourceNamespace($this->namespace);  // 设置 namespace
                $topicResource->setName($topic);
                
                // Create filter expression ✅
                $v2FilterExpression = new \Apache\Rocketmq\V2\FilterExpression();
                $v2FilterExpression->setExpression($filterExpression->getExpression());
                
                // Set filter type ✅
                $filterType = $filterExpression->getType();
                if ($filterType === \Apache\Rocketmq\Consumer\FilterExpressionType::SQL92) {
                    $v2FilterExpression->setType(\Apache\Rocketmq\V2\FilterType::SQL);
                } else {
                    $v2FilterExpression->setType(\Apache\Rocketmq\V2\FilterType::TAG);
                }
                
                // Create subscription entry
                $entry = new \Apache\Rocketmq\V2\SubscriptionEntry();
                $entry->setTopic($topicResource);
                $entry->setExpression($v2FilterExpression);
                
                $subscriptionEntries[] = $entry;
            }
        } else {
            // Fallback: use single topic (backward compatibility)
            $entry = new \Apache\Rocketmq\V2\SubscriptionEntry();
            $topicResource = new \Apache\Rocketmq\V2\Resource();
            $topicResource->setResourceNamespace($this->namespace);
            $topicResource->setName($this->topic);
            $entry->setTopic($topicResource);
            
            $subscriptionEntries[] = $entry;
        }
        
        $subscription->setSubscriptions($subscriptionEntries);
        $settings->setSubscription($subscription);
        
        return $settings;
    }
    
    /**
     * Resend settings with updated subscription expressions
     * This method can be called when subscriptions change (subscribe/unsubscribe)
     * 
     * @param array $subscriptionExpressions Topic -> FilterExpression map
     * @return void
     */
    public function resendSettings(array $subscriptionExpressions = []): void
    {
        if (!$this->isActive || !$this->streamCall) {
            Logger::warn("Cannot resend settings, session is not active, clientId={$this->clientId}");
            return;
        }
        
        // Update internal subscription expressions
        if (!empty($subscriptionExpressions)) {
            $this->subscriptionExpressions = $subscriptionExpressions;
        }
        
        try {
            // Build settings with current subscriptions
            $settings = $this->buildSettings($this->subscriptionExpressions);
            
            // Create telemetry command
            $command = new \Apache\Rocketmq\V2\TelemetryCommand();
            $command->setSettings($settings);
            
            // Write to stream
            $this->streamCall->write($command);
            
            Logger::info("Settings resent via async stream, clientId={}, subscriptionCount={}", [
                $this->clientId,
                count($this->subscriptionExpressions)
            ]);
        } catch (\Throwable $e) {
            Logger::error("Failed to resend settings, clientId={$this->clientId}, error={$e->getMessage()}");
            throw $e;
        }
    }
    
    /**
     * Update subscription expressions and resend settings
     * Convenience method that combines updating and resending
     * 
     * @param array $subscriptionExpressions Topic -> FilterExpression map
     * @return void
     */
    public function updateSubscriptions(array $subscriptionExpressions): void
    {
        $this->subscriptionExpressions = $subscriptionExpressions;
        $this->resendSettings($subscriptionExpressions);
    }
    
    /**
     * Read responses from the stream (runs in separate coroutine)
     * Aligned with Node.js TelemetrySession.on('data') handler
     * Uses non-blocking polling to avoid permanent blocking
     * 
     * @return void
     */
    private function readResponses(): void
    {
        Logger::info("Response reader coroutine started, clientId={$this->clientId}");
        
        try {
            while ($this->isActive && $this->streamCall) {
                // Use non-blocking read with timeout
                // This prevents permanent blocking if server doesn't respond immediately
                $response = $this->streamCall->read();
                
                if ($response === null) {
                    // No data available or stream closed
                    // Sleep briefly before next attempt (non-blocking poll)
                    \Swoole\Coroutine::sleep(0.1); // 100ms poll interval
                    continue;
                }
                
                // Process the telemetry command
                $this->handleTelemetryCommand($response);
            }
        } catch (\Exception $e) {
            Logger::error("Response reader error, clientId={$this->clientId}: " . $e->getMessage());
        }
        
        Logger::info("Response reader coroutine stopped, clientId={$this->clientId}");
    }
    
    /**
     * Handle incoming telemetry command
     * Aligned with Node.js BaseClient.onSettingsCommand()
     * 
     * @param \Apache\Rocketmq\V2\TelemetryCommand $command
     * @return void
     */
    private function handleTelemetryCommand(\Apache\Rocketmq\V2\TelemetryCommand $command): void
    {
        // Check for settings command
        if ($command->hasSettings()) {
            $settings = $command->getSettings();
            Logger::debug("Received settings command from server, clientId={}", [$this->clientId]);
            
            // Notify that first settings has been received (for startup synchronization)
            $this->notifyFirstSettingsReceived();
            
            // TODO: Apply settings from server if needed
            // For now, we just acknowledge receipt
        }
        
        // Handle other command types if needed
        if ($command->hasStatus()) {
            $status = $command->getStatus();
            Logger::debug("Received status from server, code={}, clientId={}", [
                $status->getCode(),
                $this->clientId
            ]);
        }
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
    
    /**
     * Wait for first settings command response from server
     * Aligned with Node.js BaseClient startup flow
     * 
     * @param int $timeout Timeout in seconds (default: 10)
     * @return bool True if received, false if timeout
     * @throws \Exception If not in coroutine context
     */
    public function waitForFirstSettings(int $timeout = 10): bool
    {
        if (!$this->startupChannel) {
            Logger::warn("Startup channel not initialized, clientId={$this->clientId}");
            return false;
        }
        
        // If already received, return immediately
        if ($this->firstSettingsReceived) {
            return true;
        }
        
        Logger::debug("Waiting for first onSettingsCommand, clientId={}, timeout={}s", [
            $this->clientId,
            $timeout
        ]);
        
        // Wait for signal with timeout
        $result = $this->startupChannel->pop($timeout);
        
        if ($result === false) {
            Logger::error("Timeout waiting for first onSettingsCommand, clientId={}, timeout={}s", [
                $this->clientId,
                $timeout
            ]);
            return false;
        }
        
        Logger::info("Received first onSettingsCommand, clientId={}", [$this->clientId]);
        return true;
    }
    
    /**
     * Notify that first settings command has been received
     * 
     * @return void
     */
    private function notifyFirstSettingsReceived(): void
    {
        Logger::debug("notifyFirstSettingsReceived called, firstSettingsReceived={}, startupChannel={}, clientId={}", [
            $this->firstSettingsReceived ? 'true' : 'false',
            $this->startupChannel ? 'exists' : 'null',
            $this->clientId
        ]);
        
        if ($this->startupChannel && !$this->firstSettingsReceived) {
            $this->firstSettingsReceived = true;
            
            // Non-blocking push
            if ($this->startupChannel->length() === 0) {
                $pushed = $this->startupChannel->push(true);
                Logger::debug("Pushed to startup channel, result={}, channelLength={}, clientId={}", [
                    $pushed ? 'success' : 'failed',
                    $this->startupChannel->length(),
                    $this->clientId
                ]);
            } else {
                Logger::warn("Startup channel already has data, skipping push, clientId={}", [$this->clientId]);
            }
        } else {
            Logger::debug("Skipping notification: startupChannel={}, firstSettingsReceived={}, clientId={}", [
                $this->startupChannel ? 'exists' : 'null',
                $this->firstSettingsReceived ? 'true' : 'false',
                $this->clientId
            ]);
        }
    }
}
