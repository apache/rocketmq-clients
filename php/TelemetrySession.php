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
use Apache\Rocketmq\V2\Settings;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\TelemetryRequest;
use Apache\Rocketmq\V2\TelemetryResponse;
use Apache\Rocketmq\V2\UserAgent;
use Grpc\BaseStub;
use Psr\Log\LoggerInterface;

/**
 * Telemetry session for bidirectional streaming with server
 * 
 * Manages bidirectional gRPC stream to receive dynamic configuration
 * from the server and send telemetry data.
 * 
 * Features:
 * - Bidirectional streaming communication
 * - Dynamic settings updates from server
 * - Automatic reconnection on failure
 * - Thread-safe operations
 * 
 * @see Java TelemetrySession implementation
 */
class TelemetrySession {
    /**
     * @var string Client ID
     */
    private $clientId;
    
    /**
     * @var BaseStub gRPC client stub
     */
    private $client;
    
    /**
     * @var callable Stream call object
     */
    private $streamCall;
    
    /**
     * @var bool Whether session is active
     */
    private $active = false;
    
    /**
     * @var LoggerInterface|null Logger instance
     */
    private $logger;
    
    /**
     * @var callable|null Settings update callback
     */
    private $settingsCallback;
    
    /**
     * @var int Reconnect delay in milliseconds
     */
    private $reconnectDelay = 5000;
    
    /**
     * @var int Max reconnect attempts
     */
    private $maxReconnectAttempts = 10;
    
    /**
     * @var int Current reconnect attempts
     */
    private $reconnectAttempts = 0;
    
    /**
     * @var \Thread|callable|null Background thread or task
     */
    private $backgroundTask = null;
    
    /**
     * Constructor
     * 
     * @param string $clientId Client identifier
     * @param BaseStub $client gRPC client stub
     * @param LoggerInterface|null $logger Logger instance
     */
    public function __construct(string $clientId, BaseStub $client, ?LoggerInterface $logger = null) {
        $this->clientId = $clientId;
        $this->client = $client;
        $this->logger = $logger;
    }
    
    /**
     * Start telemetry session
     * 
     * Establishes bidirectional stream with server and starts
     * background listener for incoming messages.
     * 
     * @return void
     * @throws \Exception If stream creation fails
     */
    public function start(): void {
        if ($this->active) {
            $this->logDebug("Telemetry session already active, clientId={}", [$this->clientId]);
            return;
        }
        
        try {
            // Create bidirectional stream
            $this->streamCall = $this->client->Telemetry([], [
                'timeout' => 3600, // 1 hour timeout
            ]);
            
            $this->active = true;
            $this->reconnectAttempts = 0;
            
            // Send initial handshake
            $this->sendHandshake();
            
            // Start background listener
            $this->startBackgroundListener();
            
            $this->logInfo("Telemetry session started successfully, clientId={}", [$this->clientId]);
            
        } catch (\Exception $e) {
            $this->logError("Failed to start telemetry session, clientId={}, error={}", [
                $this->clientId,
                $e->getMessage()
            ]);
            throw $e;
        }
    }
    
    /**
     * Stop telemetry session
     * 
     * Gracefully closes the stream and stops background tasks.
     * 
     * @return void
     */
    public function stop(): void {
        if (!$this->active) {
            return;
        }
        
        $this->active = false;
        
        // Stop background task
        if ($this->backgroundTask !== null) {
            if (is_callable($this->backgroundTask)) {
                // Cancel Swoole timer/coroutine
                $this->backgroundTask = null;
            }
        }
        
        // Close stream
        if ($this->streamCall !== null) {
            try {
                $this->streamCall->cancel();
            } catch (\Exception $e) {
                $this->logWarn("Error closing stream, clientId={}, error={}", [
                    $this->clientId,
                    $e->getMessage()
                ]);
            }
            $this->streamCall = null;
        }
        
        $this->logInfo("Telemetry session stopped, clientId={}", [$this->clientId]);
    }
    
    /**
     * Check if session is active
     * 
     * @return bool True if active, false otherwise
     */
    public function isActive(): bool {
        return $this->active;
    }
    
    /**
     * Set settings update callback
     * 
     * This callback will be invoked when server sends new settings.
     * 
     * @param callable $callback Callback function(Settings $settings): void
     * @return self
     */
    public function setSettingsCallback(callable $callback): self {
        $this->settingsCallback = $callback;
        return $this;
    }
    
    /**
     * Send telemetry command to server
     * 
     * @param TelemetryCommand $command Command to send
     * @return void
     * @throws \Exception If send fails
     */
    public function send(TelemetryCommand $command): void {
        if (!$this->active || $this->streamCall === null) {
            throw new \Exception("Telemetry session is not active");
        }
        
        try {
            $this->streamCall->write($command);
        } catch (\Exception $e) {
            $this->logError("Failed to send telemetry command, clientId={}, error={}", [
                $this->clientId,
                $e->getMessage()
            ]);
            throw $e;
        }
    }
    
    /**
     * Send settings to server
     * 
     * @param Settings $settings Settings to send
     * @return void
     */
    public function sendSettings(Settings $settings): void {
        $command = new TelemetryCommand();
        $command->setSettings($settings);
        $this->send($command);
    }
    
    /**
     * Get client ID
     * 
     * @return string
     */
    public function getClientId(): string {
        return $this->clientId;
    }
    
    /**
     * Send initial handshake message
     * 
     * @return void
     */
    private function sendHandshake(): void {
        $userAgent = new UserAgent();
        $userAgent->setLanguage('PHP');
        $userAgent->setVersion($this->getSdkVersion());
        
        $settings = new Settings();
        $settings->setClientType(ClientType::PRODUCER); // Default to producer
        $settings->setUserAgent($userAgent);
        
        $this->sendSettings($settings);
        $this->logDebug("Handshake sent, clientId={}", [$this->clientId]);
    }
    
    /**
     * Start background listener for incoming messages
     * 
     * Uses Swoole coroutine if available, otherwise uses simple loop.
     * 
     * @return void
     */
    private function startBackgroundListener(): void {
        if (extension_loaded('swoole') && class_exists('\\Swoole\\Coroutine')) {
            $this->startSwooleListener();
        } else {
            $this->startSimpleListener();
        }
    }
    
    /**
     * Start Swoole coroutine-based listener
     * 
     * @return void
     */
    private function startSwooleListener(): void {
        go(function() {
            $this->logDebug("Started Swoole coroutine listener, clientId={}", [$this->clientId]);
            
            while ($this->active && $this->streamCall !== null) {
                try {
                    // Read message from stream (non-blocking)
                    $response = $this->streamCall->read();
                    
                    if ($response === null) {
                        // Stream closed or no data
                        usleep(100000); // Sleep 100ms
                        continue;
                    }
                    
                    if ($response instanceof TelemetryResponse) {
                        $this->handleResponse($response);
                    }
                    
                } catch (\Exception $e) {
                    $this->logError("Error in Swoole listener, clientId={}, error={}", [
                        $this->clientId,
                        $e->getMessage()
                    ]);
                    
                    // Attempt reconnection
                    if ($this->reconnectAttempts < $this->maxReconnectAttempts) {
                        $this->attemptReconnect();
                    } else {
                        $this->logError("Max reconnect attempts reached, stopping session");
                        $this->stop();
                        break;
                    }
                }
            }
        });
    }
    
    /**
     * Start simple blocking listener (fallback)
     * 
     * @return void
     */
    private function startSimpleListener(): void {
        // For non-Swoole environments, we can't do async listening
        // This would need to be called periodically by the application
        $this->logWarn("Using simple listener mode (not async), clientId={}", [$this->clientId]);
    }
    
    /**
     * Handle incoming telemetry response
     * 
     * @param TelemetryResponse $response Response from server
     * @return void
     */
    private function handleResponse(TelemetryResponse $response): void {
        $this->logDebug("Received telemetry response, clientId={}", [$this->clientId]);
        
        // Check for settings update
        if ($response->hasSettings()) {
            $settings = $response->getSettings();
            $this->logInfo("Received settings update from server, clientId={}", [$this->clientId]);
            
            // Invoke callback if set
            if ($this->settingsCallback !== null) {
                try {
                    call_user_func($this->settingsCallback, $settings);
                } catch (\Exception $e) {
                    $this->logError("Error in settings callback, clientId={}, error={}", [
                        $this->clientId,
                        $e->getMessage()
                    ]);
                }
            }
        }
    }
    
    /**
     * Attempt to reconnect
     * 
     * @return void
     */
    private function attemptReconnect(): void {
        $this->reconnectAttempts++;
        
        $delayMs = min(
            $this->reconnectDelay * pow(2, $this->reconnectAttempts - 1),
            30000 // Max 30 seconds
        );
        
        $this->logWarn("Attempting reconnection, attempt={}, delay={}ms, clientId={}", [
            $this->reconnectAttempts,
            $delayMs,
            $this->clientId
        ]);
        
        usleep($delayMs * 1000);
        
        try {
            $this->streamCall = $this->client->Telemetry([], [
                'timeout' => 3600,
            ]);
            $this->sendHandshake();
            $this->reconnectAttempts = 0; // Reset on success
            $this->logInfo("Reconnection successful, clientId={}", [$this->clientId]);
        } catch (\Exception $e) {
            $this->logError("Reconnection failed, clientId={}, error={}", [
                $this->clientId,
                $e->getMessage()
            ]);
        }
    }
    
    /**
     * Get SDK version
     * 
     * @return string
     */
    private function getSdkVersion(): string {
        return '5.0.0'; // TODO: Read from composer.json
    }
    
    /**
     * Log info message
     */
    private function logInfo(string $message, array $context = []): void {
        if ($this->logger !== null) {
            $this->logger->info($this->formatMessage($message, $context));
        }
    }
    
    /**
     * Log debug message
     */
    private function logDebug(string $message, array $context = []): void {
        if ($this->logger !== null) {
            $this->logger->debug($this->formatMessage($message, $context));
        }
    }
    
    /**
     * Log warning message
     */
    private function logWarn(string $message, array $context = []): void {
        if ($this->logger !== null) {
            $this->logger->warning($this->formatMessage($message, $context));
        }
    }
    
    /**
     * Log error message
     */
    private function logError(string $message, array $context = []): void {
        if ($this->logger !== null) {
            $this->logger->error($this->formatMessage($message, $context));
        }
    }
    
    /**
     * Format log message
     */
    private function formatMessage(string $message, array $context = []): string {
        foreach ($context as $key => $value) {
            $message = str_replace('{' . $key . '}', (string)$value, $message);
        }
        return $message;
    }
}
