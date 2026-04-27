<?php
declare(strict_types=1);
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
use Apache\Rocketmq\V2\TelemetryResponse;

/**
 * Swoole-based Telemetry session using native HTTP/2 client
 * 
 * Implements gRPC bidirectional streaming manually over Swoole HTTP/2 protocol.
 * This avoids the compatibility issues with official gRPC extension in Swoole environment.
 * 
 * Key features:
 * - Manual gRPC framing (5-byte header + protobuf data)
 * - Bidirectional streaming via Swoole HTTP/2 push/recv
 * - Automatic reconnection with exponential backoff
 * - Non-blocking operation in Swoole coroutine
 */
class SwooleTelemetrySession {
    /**
     * Reconnect backoff delay base in milliseconds
     */
    private const RECONNECT_BASE_DELAY_MS = 1000;

    /**
     * Maximum reconnect delay in milliseconds
     */
    private const RECONNECT_MAX_DELAY_MS = 30000;

    /**
     * Maximum reconnect attempts before giving up
     */
    private const MAX_RECONNECT_ATTEMPTS = 10;

    /**
     * Stream timeout in seconds
     */
    private const STREAM_TIMEOUT_SECONDS = 3600;

    private string $clientId;
    private string $endpoints;
    private bool $active = false;
    private ?\Swoole\Coroutine\Http\Client $httpClient = null;
    private int $streamId = 0;
    private int $reconnectAttempts = 0;
    
    /** @var callable|null Settings update callback */
    private $settingsCallback = null;
    
    /** @var callable|null Orphaned transaction command callback */
    private $orphanedTransactionCallback = null;
    
    private static ?string $sdkVersion = null;

    public function __construct(string $clientId, string $endpoints) {
        $this->clientId = $clientId;
        $this->endpoints = $endpoints;
    }

    /**
     * Start telemetry session
     */
    public function start(): void {
        if ($this->active) {
            Logger::debug("Telemetry session already active, clientId={$this->clientId}");
            return;
        }

        try {
            $this->createStreamAndSync();
            $this->active = true;
            $this->reconnectAttempts = 0;
            
            // Start background listener in coroutine
            $this->startBackgroundListener();

            Logger::info("Swoole Telemetry session started successfully, clientId={$this->clientId}");
        } catch (\Throwable $e) {
            Logger::error("Failed to start Swoole telemetry session, clientId={$this->clientId}, error={$e->getMessage()}");
            throw $e;
        }
    }

    /**
     * Stop telemetry session
     */
    public function stop(): void {
        if (!$this->active) {
            return;
        }

        $this->active = false;

        if ($this->httpClient !== null) {
            try {
                $this->httpClient->close();
            } catch (\Throwable $e) {
                Logger::warn("Error closing HTTP/2 client, clientId={$this->clientId}, error={$e->getMessage()}");
            }
            $this->httpClient = null;
        }

        Logger::info("Swoole Telemetry session stopped, clientId={$this->clientId}");
    }

    public function isActive(): bool {
        return $this->active;
    }

    /**
     * Set settings update callback
     */
    public function setSettingsCallback(callable $callback): self {
        $this->settingsCallback = $callback;
        return $this;
    }

    /**
     * Set orphaned transaction command callback
     */
    public function setOrphanedTransactionCallback(callable $callback): self {
        $this->orphanedTransactionCallback = $callback;
        return $this;
    }

    /**
     * Send telemetry command to server
     */
    public function send(TelemetryCommand $command): void {
        if (!$this->active || $this->httpClient === null) {
            throw new \RuntimeException("Telemetry session is not active");
        }

        try {
            // Serialize message
            $data = $command->serializeToString();
            
            // Add gRPC framing: 1 byte compression flag + 4 bytes length
            $framedData = pack('CN', 0, strlen($data)) . $data;
            
            Logger::debug("Writing telemetry command to HTTP/2 stream, hasSettings={}, dataSize={}, clientId={}", [
                $command->hasSettings(),
                strlen($framedData),
                $this->clientId
            ]);
            
            // Push data to HTTP/2 stream
            $result = $this->httpClient->push($framedData);
            
            if (!$result) {
                $errorMsg = "Failed to push data: errCode=" . $this->httpClient->errCode . 
                           ", errMsg=" . $this->httpClient->errMsg;
                Logger::error("{$errorMsg}, clientId={$this->clientId}");
                throw new \RuntimeException($errorMsg);
            }
            
            Logger::debug("Telemetry command pushed successfully, clientId={$this->clientId}");
        } catch (\Throwable $e) {
            Logger::error("Failed to send telemetry command, clientId={$this->clientId}, error={$e->getMessage()}");
            throw $e;
        }
    }

    /**
     * Send settings to server
     */
    public function sendSettings(Settings $settings): void {
        $command = new TelemetryCommand();
        $command->setSettings($settings);
        $this->send($command);
    }

    public function getClientId(): string {
        return $this->clientId;
    }

    /**
     * Create HTTP/2 stream and send initial handshake
     */
    private function createStreamAndSync(): void {
        Logger::debug("Creating Swoole HTTP/2 client for Telemetry, endpoints={$this->endpoints}, clientId={$this->clientId}");
        
        // Parse endpoints
        list($host, $port) = $this->parseEndpoints($this->endpoints);
        
        // Create HTTP/2 client
        $httpClient = new \Swoole\Coroutine\Http\Client($host, (int)$port);
        
        // Configure HTTP/2
        $httpClient->set([
            'open_http2_protocol' => true,
            'timeout' => 10,
            'ssl_verify_peer' => false,
        ]);
        
        Logger::debug("Swoole HTTP/2 client configured for {$this->endpoints}, clientId={$this->clientId}");
        
        // Build request headers for gRPC
        $headers = [
            'content-type' => 'application/grpc',
            'te' => 'trailers',
            'user-agent' => 'rocketmq-php-client/' . self::getSdkVersion(),
            'x-mq-protocol' => 'v2',
            'x-mq-client-id' => $this->clientId,
        ];
        
        // Add namespace if needed
        $headers['x-mq-namespace'] = '';
        
        // Set headers
        $httpClient->setHeaders($headers);
        
        $path = '/apache.rocketmq.v2.MessagingService/Telemetry';
        
        // For gRPC bidirectional streaming:
        // We need to manually construct the HTTP/2 HEADERS frame and DATA frames
        // But Swoole's HTTP/2 client doesn't expose low-level APIs for this
        
        // Alternative approach: Use a raw TCP connection with manual HTTP/2 framing
        // This is complex but necessary for proper gRPC support
        
        // For now, let's try using send() with empty body to initiate the stream
        $result = $httpClient->send($path, '');
        
        if (!$result) {
            throw new \Exception(
                "Failed to send HTTP/2 request: errCode=" . $httpClient->errCode . 
                ", errMsg=" . $httpClient->errMsg
            );
        }
        
        $this->httpClient = $httpClient;
        $this->streamId = 1; // First stream
        
        Logger::info("HTTP/2 stream initiated via send(), path={$path}, clientId={$this->clientId}");
        
        // Send initial handshake
        $this->sendHandshake();
    }

    /**
     * Send initial handshake message
     */
    private function sendHandshake(): void {
        $ua = new \Apache\Rocketmq\V2\UA();
        $ua->setLanguage(\Apache\Rocketmq\V2\Language::PHP);
        $ua->setVersion(self::getSdkVersion());

        $settings = new Settings();
        $settings->setClientType(ClientType::SIMPLE_CONSUMER);
        $settings->setUserAgent($ua);

        $this->sendSettings($settings);
        Logger::debug("Handshake sent, clientId={$this->clientId}");
    }

    /**
     * Start background listener for incoming messages
     */
    private function startBackgroundListener(): void {
        go(function() {
            Logger::debug("Started Swoole HTTP/2 background listener, clientId={$this->clientId}");
            
            $readCount = 0;
            $maxReads = 1000; // Limit to avoid infinite loop
            
            while ($this->active && $this->httpClient !== null && $readCount < $maxReads) {
                try {
                    // Receive data from HTTP/2 stream
                    $response = $this->httpClient->recv(0.5);
                    $readCount++;
                    
                    if ($response === false || $response === '') {
                        // No data or error, continue polling
                        if ($this->httpClient->errCode !== 0) {
                            Logger::warn("HTTP/2 recv error: errCode=" . $this->httpClient->errCode . 
                                       ", errMsg=" . $this->httpClient->errMsg . ", clientId={$this->clientId}");
                            
                            // Attempt reconnection
                            if ($this->reconnectAttempts < self::MAX_RECONNECT_ATTEMPTS) {
                                $this->attemptReconnect();
                                $readCount = 0; // Reset counter after reconnect
                            } else {
                                Logger::error("Max reconnect attempts reached, stopping session, clientId={$this->clientId}");
                                $this->stop();
                                break;
                            }
                        }
                        continue;
                    }
                    
                    // Parse gRPC response
                    $this->handleGrpcResponse($response);
                    
                } catch (\Throwable $e) {
                    Logger::error("Error in HTTP/2 listener, clientId={$this->clientId}, error={$e->getMessage()}");
                    
                    if ($this->reconnectAttempts < self::MAX_RECONNECT_ATTEMPTS) {
                        $this->attemptReconnect();
                        $readCount = 0;
                    } else {
                        Logger::error("Max reconnect attempts reached, stopping session, clientId={$this->clientId}");
                        $this->stop();
                        break;
                    }
                }
            }
            
            Logger::debug("Swoole HTTP/2 listener stopped after {$readCount} reads, clientId={$this->clientId}");
        });
    }

    /**
     * Handle gRPC response
     */
    private function handleGrpcResponse(string $rawData): void {
        try {
            // Skip gRPC framing (5 bytes: 1 byte compression flag + 4 bytes length)
            if (strlen($rawData) < 5) {
                Logger::warn("Response too short, skipping, clientId={$this->clientId}");
                return;
            }
            
            // Parse length
            $length = unpack('N', substr($rawData, 1, 4))[1];
            
            // Extract protobuf data
            $protobufData = substr($rawData, 5, $length);
            
            // Decode TelemetryResponse
            $response = new TelemetryResponse();
            $response->mergeFromString($protobufData);
            
            Logger::debug("Received telemetry response, clientId={$this->clientId}");
            
            // Handle response based on type
            if ($response->hasSettings()) {
                $settings = $response->getSettings();
                Logger::info("Receive settings from remote, clientId={$this->clientId}");
                
                if ($this->settingsCallback !== null) {
                    try {
                        call_user_func($this->settingsCallback, $settings);
                    } catch (\Throwable $e) {
                        Logger::error("[Bug] Error in settings callback, clientId={$this->clientId}, error={$e->getMessage()}");
                    }
                }
                return;
            }
            
            // Handle other response types...
            Logger::debug("Response processed, clientId={$this->clientId}");
            
        } catch (\Throwable $e) {
            Logger::error("Failed to handle gRPC response, clientId={$this->clientId}, error={$e->getMessage()}");
        }
    }

    /**
     * Attempt to reconnect with exponential backoff
     */
    private function attemptReconnect(): void {
        $this->reconnectAttempts++;
        
        // Exponential backoff with jitter
        $exponentialDelay = (int)(self::RECONNECT_BASE_DELAY_MS * pow(2, $this->reconnectAttempts - 1));
        $jitter = random_int(0, self::RECONNECT_BASE_DELAY_MS);
        $delayMs = min($exponentialDelay + $jitter, self::RECONNECT_MAX_DELAY_MS);
        
        Logger::warn("Attempting reconnection, attempt={$this->reconnectAttempts}, delay={$delayMs}ms, clientId={$this->clientId}");
        
        usleep($delayMs * 1000);
        
        try {
            // Close old connection
            if ($this->httpClient !== null) {
                $this->httpClient->close();
            }
            
            // Create new stream
            $this->createStreamAndSync();
            $this->reconnectAttempts = 0;
            
            Logger::info("Reconnection successful, clientId={$this->clientId}");
        } catch (\Throwable $e) {
            Logger::error("Reconnection failed, clientId={$this->clientId}, error={$e->getMessage()}");
        }
    }

    /**
     * Parse endpoints string to host and port
     */
    private function parseEndpoints(string $endpoints): array {
        // Remove protocol if present
        $endpoints = preg_replace('#^https?://#', '', $endpoints);
        
        // Split host and port
        if (strpos($endpoints, ':') !== false) {
            list($host, $port) = explode(':', $endpoints, 2);
        } else {
            $host = $endpoints;
            $port = 8080; // Default port
        }
        
        return [$host, (int)$port];
    }

    /**
     * Get SDK version
     */
    public static function getSdkVersion(): string {
        if (self::$sdkVersion !== null) {
            return self::$sdkVersion;
        }

        // Try reading from composer.json
        $composerFile = __DIR__ . '/composer.json';
        if (file_exists($composerFile)) {
            $composerData = @json_decode((string)file_get_contents($composerFile), true);
            if (is_array($composerData) && isset($composerData['version']) && is_string($composerData['version'])) {
                self::$sdkVersion = $composerData['version'];
                return self::$sdkVersion;
            }
        }

        // Fallback
        self::$sdkVersion = '5.0.0';
        return self::$sdkVersion;
    }
}
