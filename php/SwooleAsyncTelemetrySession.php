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

/**
 * AsyncTelemetrySession using Swoole HTTP/2 for non-blocking bidirectional streams
 * 
 * This implementation uses Swoole's native HTTP/2 client to create true async
 * non-blocking gRPC bidirectional streams, avoiding the blocking issues
 * of the official PHP gRPC extension.
 * 
 * Requirements:
 * - Swoole extension >= 4.5.3 (for HTTP/2 streaming support)
 * - PHP >= 7.4
 */
class SwooleAsyncTelemetrySession
{
    private $clientId;
    private $consumerGroup;
    private $topic;
    private $namespace;
    private $clientType;
    private $longPollingTimeout;
    private $endpoints;
    
    /** @var array<string, \Apache\Rocketmq\Consumer\FilterExpression> Subscription expressions */
    private $subscriptionExpressions = [];
    
    /** @var SwooleStreamingCall|null Bidirectional streaming call */
    private $streamCall = null;
    
    /** @var \Swoole\Coroutine\Http\Client|null Swoole HTTP/2 client */
    private $httpClient = null;
    
    /** @var int Stream ID */
    private $streamId = 0;
    
    private $isActive = false;
    private $running = false;
    
    /**
     * @var int Reconnect backoff delay in seconds
     */
    const RECONNECT_DELAY = 1;
    
    /**
     * @var int Receive timeout in seconds (non-blocking)
     */
    const RECEIVE_TIMEOUT = 1;
    
    /**
     * Constructor
     * 
     * @param string $endpoints Server endpoints (e.g., "127.0.0.1:8080")
     * @param string $clientId Client ID
     * @param string $consumerGroup Consumer group name
     * @param string $topic Topic name
     * @param int $clientType Client type constant
     * @param int $longPollingTimeout Long polling timeout in seconds
     * @param string $namespace Namespace (optional, defaults to empty)
     */
    public function __construct(
        string $endpoints,
        string $clientId,
        string $consumerGroup,
        string $topic,
        int $clientType,
        int $longPollingTimeout = 30,
        string $namespace = ''
    ) {
        if (!extension_loaded('swoole')) {
            throw new \RuntimeException(
                "Swoole extension is required for SwooleAsyncTelemetrySession. " .
                "Please install it: pecl install swoole"
            );
        }
        
        $this->endpoints = $endpoints;
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
                "SwooleAsyncTelemetrySession requires Swoole extension. " .
                "Install it with: pecl install swoole"
            );
        }
        
        $this->running = true;
        
        // Create a coroutine to run the session
        go(function() {
            $this->run();
        });
        
        Logger::info("SwooleAsyncTelemetrySession started in coroutine, clientId={$this->clientId}");
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
                // Create stream and sync settings
                $this->createStreamAndSync();
                
                // Keep the session alive and process responses
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
     * Uses swoole/grpc for non-blocking bidirectional stream
     * 
     * @return void
     * @throws \Exception If stream creation fails
     */
    private function createStreamAndSync(): void
    {
        Logger::debug("Creating Swoole gRPC client for Telemetry, endpoints={}, clientId={}", [
            $this->endpoints,
            $this->clientId
        ]);
            
        // Parse endpoints
        list($host, $port) = explode(':', $this->endpoints);
        $port = intval($port);
            
        // Create Swoole HTTP/2 client (not gRPC Client, to avoid API conflicts)
        $httpClient = new \Swoole\Coroutine\Http\Client($host, $port);
            
        if (!$httpClient) {
            throw new \Exception("Failed to create Swoole HTTP/2 client");
        }
            
        // Configure HTTP/2
        $httpClient->set([
            'open_http2_protocol' => true,
            'timeout' => 10,
            'ssl_verify_peer' => false,
        ]);
            
        // In Swoole 5.x, connection is established automatically when calling send()
        // No need to call connect() explicitly
        Logger::debug("Swoole HTTP/2 client configured for {$this->endpoints}, clientId={}", [$this->clientId]);
            
        // Upgrade to gRPC stream using HTTP/2
        $path = '/apache.rocketmq.v2.MessagingService/Telemetry';
            
        // Build request headers
        $headers = [
            'content-type' => 'application/grpc',
            'te' => 'trailers',
            'user-agent' => 'rocketmq-php-client/' . Util::getSdkVersion(),
            'x-mq-protocol' => 'v2',
            'x-mq-client-id' => $this->clientId,
        ];
            
        // Add namespace if set
        if (!empty($this->namespace)) {
            $headers['x-mq-namespace'] = $this->namespace;
        }
            
        // Start streaming request
        $streamId = $httpClient->send($path, '', $headers);
            
        if ($streamId <= 0) {
            throw new \Exception("Failed to start gRPC stream: errCode=" . $httpClient->errCode);
        }
            
        $this->httpClient = $httpClient;
        $this->streamId = $streamId;
        
        // Create SwooleStreamingCall for bidirectional communication
        $this->streamCall = new SwooleStreamingCall(
            $httpClient,
            'Telemetry',
            function($data) {
                $command = new \Apache\Rocketmq\V2\TelemetryCommand();
                $command->mergeFromString($data);
                return $command;
            }
        );
        
        $this->isActive = true;
            
        Logger::info("Telemetry stream created via Swoole HTTP/2, streamId={}, clientId={}", [
            $this->streamId,
            $this->clientId
        ]);
            
        // Send initial settings
        Logger::debug("Sending initial settings..., clientId={}", [$this->clientId]);
        $this->sendSettings();
        Logger::debug("Initial settings sent, clientId={}", [$this->clientId]);
    }
    
    /**
     * Send settings to the server
     * 
     * @return void
     * @throws \Exception If sending fails
     */
    private function sendSettings(): void
    {
        if (!$this->isActive || !$this->httpClient || $this->streamId <= 0) {
            Logger::warn("Cannot send settings, session is not active, clientId={$this->clientId}");
            return;
        }
        
        // Build settings
        $settings = $this->buildSettings();
        
        // Create telemetry command
        $command = new \Apache\Rocketmq\V2\TelemetryCommand();
        $command->setSettings($settings);
        
        // Serialize message with gRPC framing
        $data = $command->serializeToString();
        $framedData = pack('CN', 0, strlen($data)) . $data;
        
        // Write to stream
        $result = $this->httpClient->write($this->streamId, $framedData, false);
        
        if (!$result) {
            throw new \Exception("Failed to send settings: errCode=" . $this->httpClient->errCode);
        }
        
        Logger::info("Settings sent via Swoole HTTP/2 stream, clientId={$this->clientId}");
    }
    
    /**
     * Send custom settings (public API for external calls)
     * 
     * @param \Apache\Rocketmq\V2\Settings $settings Settings object
     * @return void
     */
    public function sendCustomSettings(\Apache\Rocketmq\V2\Settings $settings): void
    {
        if (!$this->isActive || !$this->httpClient || $this->streamId <= 0) {
            Logger::warn("Cannot send custom settings, session is not active, clientId={$this->clientId}");
            return;
        }
        
        $command = new \Apache\Rocketmq\V2\TelemetryCommand();
        $command->setSettings($settings);
        
        // Serialize with gRPC framing
        $data = $command->serializeToString();
        $framedData = pack('CN', 0, strlen($data)) . $data;
        
        $result = $this->httpClient->write($this->streamId, $framedData, false);
        
        if (!$result) {
            Logger::error("Failed to send custom settings, clientId={$this->clientId}");
            return;
        }
        
        Logger::info("Custom settings sent via Swoole HTTP/2 stream, clientId={$this->clientId}");
    }
    
    /**
     * Keep the session alive and process responses
     * Uses non-blocking recv with timeout to avoid permanent blocking
     * 
     * @return void
     */
    private function keepAlive(): void
    {
        Logger::info("Starting response processing loop, clientId={$this->clientId}");
        
        $pollCount = 0;
        while ($this->isActive && $this->running && $this->httpClient && $this->streamId > 0) {
            try {
                // Non-blocking receive with short timeout (0.5 second)
                $response = $this->httpClient->recv($this->streamId, 0.5);
                
                if ($response === false) {
                    // Error or stream closed
                    $errCode = $this->httpClient->errCode;
                    $errMsg = $this->httpClient->errMsg;
                    
                    if ($errCode !== 0) {
                        Logger::warn("Stream error, errCode={$errCode}, errMsg={$errMsg}, clientId={$this->clientId}");
                    }
                    
                    // Check if stream still exists
                    if (!$this->httpClient->isStreamExist($this->streamId)) {
                        Logger::warn("Stream lost after {$pollCount} polls, clientId={$this->clientId}");
                        break;
                    }
                    
                    // Continue polling
                    continue;
                }
                
                if ($response === null || strlen($response) === 0) {
                    // No data, continue polling
                    $pollCount++;
                    continue;
                }
                
                // Parse gRPC frame (skip 5-byte header: 1 byte compression flag + 4 bytes length)
                if (strlen($response) > 5) {
                    $data = substr($response, 5);
                    
                    // Deserialize telemetry command
                    $command = new \Apache\Rocketmq\V2\TelemetryCommand();
                    $command->mergeFromString($data);
                    
                    // Process the telemetry command
                    $this->handleTelemetryCommand($command);
                }
                
            } catch (\Exception $e) {
                Logger::error("Error in response processing, clientId={$this->clientId}: " . $e->getMessage());
                break;
            }
        }
        
        Logger::info("Response processing loop stopped after {$pollCount} polls, clientId={$this->clientId}");
    }
    
    /**
     * Handle incoming telemetry command
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
     * Build Settings object
     * 
     * @return \Apache\Rocketmq\V2\Settings
     */
    private function buildSettings(): \Apache\Rocketmq\V2\Settings
    {
        $settings = new \Apache\Rocketmq\V2\Settings();
        $settings->setClientType($this->clientType);
        
        // Set user agent
        $ua = new \Apache\Rocketmq\V2\UA();
        $ua->setLanguage(\Apache\Rocketmq\V2\Language::PHP);
        $ua->setVersion(Util::getSdkVersion());
        $settings->setUserAgent($ua);
        
        // Build subscription
        $subscription = new \Apache\Rocketmq\V2\Subscription();
        
        // Set group with namespace
        $group = new \Apache\Rocketmq\V2\Resource();
        $group->setResourceNamespace($this->namespace);
        $group->setName($this->consumerGroup);
        $subscription->setGroup($group);
        
        // Set long polling timeout
        $longPollingDuration = new \Google\Protobuf\Duration();
        $longPollingDuration->setSeconds($this->longPollingTimeout);
        $subscription->setLongPollingTimeout($longPollingDuration);
        
        // Build subscription entries
        $subscriptionEntries = [];
        if (!empty($this->subscriptionExpressions)) {
            foreach ($this->subscriptionExpressions as $topic => $filterExpression) {
                $topicResource = new \Apache\Rocketmq\V2\Resource();
                $topicResource->setResourceNamespace($this->namespace);
                $topicResource->setName($topic);
                
                $v2FilterExpression = new \Apache\Rocketmq\V2\FilterExpression();
                $v2FilterExpression->setExpression($filterExpression->getExpression());
                
                $filterType = $filterExpression->getType();
                if ($filterType === \Apache\Rocketmq\Consumer\FilterExpressionType::SQL92) {
                    $v2FilterExpression->setType(\Apache\Rocketmq\V2\FilterType::SQL);
                } else {
                    $v2FilterExpression->setType(\Apache\Rocketmq\V2\FilterType::TAG);
                }
                
                $entry = new \Apache\Rocketmq\V2\SubscriptionEntry();
                $entry->setTopic($topicResource);
                $entry->setExpression($v2FilterExpression);
                
                $subscriptionEntries[] = $entry;
            }
        } else {
            // Fallback: use single topic
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
     * Stop the telemetry session
     * 
     * @return void
     */
    public function stop(): void
    {
        $this->running = false;
        $this->isActive = false;
        
        // End the stream
        if ($this->httpClient && $this->streamId > 0) {
            try {
                $this->httpClient->write($this->streamId, null, true);
            } catch (\Exception $e) {
                Logger::error("Error ending stream: " . $e->getMessage());
            }
        }
        
        // Close HTTP/2 client
        if ($this->httpClient) {
            try {
                $this->httpClient->close();
            } catch (\Exception $e) {
                Logger::error("Error closing HTTP/2 client: " . $e->getMessage());
            }
        }
        
        $this->streamCall = null;
        $this->httpClient = null;
        $this->streamId = 0;
        
        Logger::info("SwooleAsyncTelemetrySession stopped, clientId={$this->clientId}");
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
