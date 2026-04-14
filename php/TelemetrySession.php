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

use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;
use Apache\Rocketmq\V2\TelemetryCommand;
use Grpc\BaseStub;

/**
 * Telemetry Session for managing bidirectional gRPC stream
 * 
 * This class maintains a persistent bidirectional stream with RocketMQ server
 * to send client settings and receive configuration updates.
 */
class TelemetrySession
{
    /**
     * @var BaseStub gRPC client stub
     */
    private $client;
    
    /**
     * @var string Client ID
     */
    private $clientId;
    
    /**
     * @var string Consumer group
     */
    private $consumerGroup;
    
    /**
     * @var string Topic name
     */
    private $topic;
    
    /**
     * @var ClientType Client type (PUSH_CONSUMER or SIMPLE_CONSUMER)
     */
    private $clientType;
    
    /**
     * @var resource|\Grpc\Call Bidirectional stream call object
     */
    private $streamCall = null;
    
    /**
     * @var bool Whether the session is active
     */
    private $isActive = false;
    
    /**
     * @var int Long polling timeout in seconds
     */
    private $longPollingTimeout = 30;
    
    /**
     * @var array Subscription entries
     */
    private $subscriptions = [];
    
    /**
     * @var int Reconnect backoff delay in seconds (default: 1 second)
     */
    private const RECONNECT_BACKOFF_DELAY = 1;
    
    /**
     * @var int Maximum reconnect attempts (0 = unlimited)
     */
    private const MAX_RECONNECT_ATTEMPTS = 0;
    
    /**
     * @var int Current reconnect attempt count
     */
    private $reconnectAttempts = 0;
    
    /**
     * @var bool Whether auto-reconnect is enabled
     */
    private $autoReconnect = true;
    
    /**
     * Constructor
     * 
     * @param BaseStub $client gRPC client stub
     * @param string $clientId Client ID
     * @param string $consumerGroup Consumer group name
     * @param string $topic Topic name
     * @param int $clientType Client type (use ClientType constants)
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
        $this->client = $client;
        $this->clientId = $clientId;
        $this->consumerGroup = $consumerGroup;
        $this->topic = $topic;
        $this->clientType = $clientType;
        $this->longPollingTimeout = $longPollingTimeout;
        
        // Add subscription for the topic
        $this->addSubscription($topic);
    }
    
    /**
     * Add a subscription entry
     * 
     * @param string $topic Topic name
     * @param string|null $expression Filter expression (default: *)
     * @return void
     */
    public function addSubscription(string $topic, ?string $expression = '*'): void
    {
        $this->subscriptions[$topic] = $expression;
    }
    
    /**
     * Remove a subscription entry
     * 
     * @param string $topic Topic name
     * @return void
     */
    public function removeSubscription(string $topic): void
    {
        unset($this->subscriptions[$topic]);
    }
    
    /**
     * Start the telemetry session (create bidirectional stream)
     * 
     * @return void
     * @throws \Exception If stream creation fails
     */
    public function start(): void
    {
        if ($this->isActive) {
            return;
        }
        
        try {
            // Create bidirectional stream
            $this->streamCall = $this->client->Telemetry();
            
            // Mark as active BEFORE sending settings
            $this->isActive = true;
            
            // Send initial settings
            $this->sendSettings();
            
            // Start background reader to keep stream alive
            $this->startBackgroundReader();
            
            Logger::info("Telemetry session started successfully, clientId={}", [$this->clientId]);
        } catch (\Exception $e) {
            // Rollback state on failure
            $this->isActive = false;
            $this->streamCall = null;
            
            throw new \Exception(
                "Failed to start telemetry session: " . $e->getMessage(),
                0,
                $e
            );
        }
    }
    
    /**
     * Start background reader to keep the stream alive
     * This prevents the server from closing the connection
     * 
     * @return void
     */
    private function startBackgroundReader(): void
    {
        // Use a separate thread/process to read responses
        // For PHP, we'll use pcntl_fork if available, otherwise skip
        if (function_exists('pcntl_fork')) {
            $pid = pcntl_fork();
            
            if ($pid == -1) {
                Logger::error("Failed to fork process for telemetry reader, clientId={}", [$this->clientId]);
                return;
            } elseif ($pid > 0) {
                // Parent process - continue normally
                Logger::info("Started telemetry background reader (PID: {}), clientId={}", [$pid, $this->clientId]);
                return;
            } else {
                // Child process - read responses continuously
                $this->runBackgroundReader();
                exit(0);
            }
        } else {
            // pcntl not available, use non-blocking read in main process
            Logger::warn("pcntl_fork not available, telemetry will use non-blocking reads, clientId={}", [$this->clientId]);
        }
    }
    
    /**
     * Run background reader loop (executed in child process)
     * 
     * @return void
     */
    private function runBackgroundReader(): void
    {
        Logger::info("Background telemetry reader started, clientId={}", [$this->clientId]);
        
        // For PHP gRPC, we can't easily read from the stream in a background process
        // because the stream call object can't be shared across processes.
        // Instead, we'll just keep the process alive to maintain the connection.
        
        $keepAliveCount = 0;
        while ($this->isActive) {
            sleep(5); // Keep alive for 5 seconds intervals
            $keepAliveCount++;
            
            if ($keepAliveCount % 12 === 0) { // Every minute
                Logger::debug("Telemetry session keep-alive ({$keepAliveCount} cycles), clientId={$this->clientId}");
            }
        }
        
        Logger::info("Background telemetry reader stopped, clientId={$this->clientId}");
    }
    
    /**
     * Send settings to the server
     * 
     * @return void
     * @throws \Exception If sending fails
     */
    public function sendSettings(): void
    {
        if (!$this->isActive || !$this->streamCall) {
            throw new \Exception("Telemetry session is not active");
        }
        
        try {
            $settings = $this->buildSettings();
            $command = new TelemetryCommand();
            $command->setSettings($settings);
            
            // Write to stream (non-blocking)
            $this->streamCall->write($command);
            
            Logger::info("Settings sent to server, clientId={$this->clientId}, consumerGroup={$this->consumerGroup}");
        } catch (\Exception $e) {
            Logger::error("Failed to send settings, clientId={$this->clientId}", ['error' => $e->getMessage()]);
            throw $e;
        }
    }
    
    /**
     * Build Settings object
     * 
     * @return Settings
     */
    private function buildSettings(): Settings
    {
        $settings = new Settings();
        
        // Set client type
        $settings->setClientType($this->clientType);
        
        // Build subscription
        $subscription = new Subscription();
        
        // Set consumer group
        $group = new Resource();
        $group->setName($this->consumerGroup);
        $subscription->setGroup($group);
        
        // Set long polling timeout
        $longPollingDuration = new \Google\Protobuf\Duration();
        $longPollingDuration->setSeconds($this->longPollingTimeout);
        $subscription->setLongPollingTimeout($longPollingDuration);
        
        // Add subscription entries
        $subscriptionEntries = [];
        foreach ($this->subscriptions as $topic => $expression) {
            $entry = new SubscriptionEntry();
            $topicResource = new Resource();
            $topicResource->setName($topic);
            $entry->setTopic($topicResource);
            
            if (!empty($expression)) {
                $filterExpression = new \Apache\Rocketmq\V2\FilterExpression();
                $filterExpression->setExpression($expression);
                $filterExpression->setType(\Apache\Rocketmq\V2\FilterType::TAG);
                $entry->setExpression($filterExpression);
            }
            
            $subscriptionEntries[] = $entry;
        }
        
        // Set all subscription entries at once
        if (!empty($subscriptionEntries)) {
            $subscription->setSubscriptions($subscriptionEntries);
        }
        
        $settings->setSubscription($subscription);
        
        return $settings;
    }
    
    /**
     * Read responses from the stream (non-blocking check)
     * 
     * @return array Array of TelemetryCommand objects received
     */
    public function readResponses(): array
    {
        $commands = [];
        
        if (!$this->isActive || !$this->streamCall) {
            return $commands;
        }
        
        try {
            // Try to read available responses
            foreach ($this->streamCall->responses() as $response) {
                if ($response instanceof TelemetryCommand) {
                    $commands[] = $response;
                    
                    // Handle different command types
                    $this->handleCommand($response);
                }
            }
        } catch (\Exception $e) {
            Logger::error("Error reading from telemetry stream, clientId={$this->clientId}", ['error' => $e->getMessage()]);
            
            // Trigger auto-reconnect if enabled
            if ($this->autoReconnect) {
                $this->handleStreamError($e);
            }
        }
        
        return $commands;
    }
    
    /**
     * Handle incoming telemetry command
     * 
     * @param TelemetryCommand $command
     * @return void
     */
    private function handleCommand(TelemetryCommand $command): void
    {
        // Check what type of command we received
        if ($command->hasSettings()) {
            Logger::debug("Received settings from server, clientId={$this->clientId}");
            // Server is sending us updated settings - we can sync them here
        }
        
        // Handle other command types as needed:
        // - RecoverOrphanedTransactionCommand
        // - VerifyMessageCommand
        // - PrintThreadStackTraceCommand
        // - ReconnectEndpointsCommand
        // - NotifyUnsubscribeLiteCommand
    }
    
    /**
     * Handle stream error and trigger auto-reconnect
     * 
     * @param \Exception $error The error that occurred
     * @return void
     */
    private function handleStreamError(\Exception $error): void
    {
        Logger::error("Telemetry stream error detected, clientId={$this->clientId}, error: " . $error->getMessage());
        
        // Mark session as inactive
        $this->isActive = false;
        $this->streamCall = null;
        
        // Attempt to reconnect
        $this->attemptReconnect();
    }
    
    /**
     * Attempt to reconnect the telemetry session
     * 
     * @return void
     */
    private function attemptReconnect(): void
    {
        // Check if auto-reconnect is enabled
        if (!$this->autoReconnect) {
            Logger::warn("Auto-reconnect is disabled, not attempting to reconnect, clientId={$this->clientId}");
            return;
        }
        
        // Check max reconnect attempts (0 = unlimited)
        if (self::MAX_RECONNECT_ATTEMPTS > 0 && $this->reconnectAttempts >= self::MAX_RECONNECT_ATTEMPTS) {
            Logger::error("Maximum reconnect attempts reached ({$this->reconnectAttempts}), giving up, clientId={$this->clientId}");
            return;
        }
        
        $this->reconnectAttempts++;
        
        Logger::info(
            "Attempting to reconnect telemetry session, " .
            "attempt={$this->reconnectAttempts}, " .
            "delay=" . self::RECONNECT_BACKOFF_DELAY . "s, " .
            "clientId={$this->clientId}"
        );
        
        // Wait for backoff delay
        sleep(self::RECONNECT_BACKOFF_DELAY);
        
        try {
            // Try to recreate the stream
            $this->recreateStream();
            
            // Reset reconnect counter on success
            $this->reconnectAttempts = 0;
            
            Logger::info(
                "Telemetry session reconnected successfully, clientId={}",
                [$this->clientId]
            );
        } catch (\Exception $e) {
            Logger::error(
                "Failed to reconnect telemetry session, attempt={}, error: {}",
                [$this->reconnectAttempts, $e->getMessage()]
            );
            
            // Schedule another reconnect attempt
            $this->attemptReconnect();
        }
    }
    
    /**
     * Recreate the gRPC stream and resend settings
     * 
     * @return void
     * @throws \Exception If stream creation fails
     */
    private function recreateStream(): void
    {
        try {
            // Create new bidirectional stream
            $this->streamCall = $this->client->Telemetry();
            
            // Resend initial settings
            $this->sendSettings();
            
            // Mark as active
            $this->isActive = true;
        } catch (\Exception $e) {
            $this->isActive = false;
            $this->streamCall = null;
            throw $e;
        }
    }
    
    /**
     * Close the telemetry session
     * 
     * @return void
     */
    public function close(): void
    {
        if (!$this->isActive) {
            return;
        }
        
        try {
            if ($this->streamCall) {
                // Signal completion
                $this->streamCall->writesDone();
            }
            
            $this->isActive = false;
            $this->streamCall = null;
            
            Logger::info("Telemetry session closed, clientId={}", [$this->clientId]);
        } catch (\Exception $e) {
            Logger::error("Error closing telemetry session, clientId={}", [$this->clientId, 'error' => $e->getMessage()]);
        }
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
     * Get the client ID
     * 
     * @return string
     */
    public function getClientId(): string
    {
        return $this->clientId;
    }
    
    /**
     * Get the consumer group
     * 
     * @return string
     */
    public function getConsumerGroup(): string
    {
        return $this->consumerGroup;
    }
    
    /**
     * Enable or disable auto-reconnect
     * 
     * @param bool $enabled Whether to enable auto-reconnect
     * @return void
     */
    public function setAutoReconnect(bool $enabled): void
    {
        $this->autoReconnect = $enabled;
        Logger::info("Auto-reconnect " . ($enabled ? "enabled" : "disabled") . " for clientId={$this->clientId}");
    }
    
    /**
     * Check if auto-reconnect is enabled
     * 
     * @return bool
     */
    public function isAutoReconnectEnabled(): bool
    {
        return $this->autoReconnect;
    }
    
    /**
     * Get current reconnect attempt count
     * 
     * @return int
     */
    public function getReconnectAttempts(): int
    {
        return $this->reconnectAttempts;
    }
    
    /**
     * Reset reconnect attempt counter
     * 
     * @return void
     */
    public function resetReconnectAttempts(): void
    {
        $this->reconnectAttempts = 0;
    }
}
