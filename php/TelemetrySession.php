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
use Apache\Rocketmq\V2\TelemetryRequest;
use Apache\Rocketmq\V2\TelemetryResponse;
use Apache\Rocketmq\V2\UserAgent;
use Grpc\BaseStub;

/**
 * Telemetry session for bidirectional streaming with server
 * 
 * Manages bidirectional gRPC stream to receive dynamic configuration
 * from the server and send telemetry data.
 * 
 * References Java ClientSessionImpl:
 * - Command routing via onNext switch (SETTINGS, RECOVER_ORPHANED_TRANSACTION, etc.)
 * - Automatic reconnection with backoff + jitter (prevents thundering herd)
 * - Settings initialization future pattern
 * 
 * @see Java ClientSessionImpl implementation
 */
class TelemetrySession {
    /**
     * Reconnect backoff delay base in milliseconds (Java: REQUEST_OBSERVER_RENEW_BACKOFF_DELAY = 1s)
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
     * Stream timeout in seconds (Java: TELEMETRY_TIMEOUT = 60 * 365 days, we use 1 hour)
     */
    private const STREAM_TIMEOUT_SECONDS = 3600;

    private string $clientId;
    private BaseStub $client;
    /** @var callable|null */
    private $streamCall = null;
    private bool $active = false;
    /** @var callable|null Settings update callback */
    private $settingsCallback = null;
    /** @var callable|null Orphaned transaction command callback */
    private $orphanedTransactionCallback = null;
    private int $reconnectAttempts = 0;
    /** @var callable|null */
    private $backgroundTask = null;
    private static ?string $sdkVersion = null;

    public function __construct(string $clientId, BaseStub $client) {
        $this->clientId = $clientId;
        $this->client = $client;
    }

    /**
     * Start telemetry session
     * 
     * Establishes bidirectional stream with server and starts
     * background listener for incoming messages.
     */
    public function start(): void {
        if ($this->active) {
            Logger::debug("Telemetry session already active, clientId={$this->clientId}");
            return;
        }

        try {
            $this->streamCall = $this->client->Telemetry([], [
                'timeout' => self::STREAM_TIMEOUT_SECONDS,
            ]);

            $this->active = true;
            $this->reconnectAttempts = 0;

            $this->sendHandshake();
            $this->startBackgroundListener();

            Logger::info("Telemetry session started successfully, clientId={$this->clientId}");
        } catch (\Throwable $e) {
            Logger::error("Failed to start telemetry session, clientId={$this->clientId}, error={$e->getMessage()}");
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
        $this->backgroundTask = null;

        if ($this->streamCall !== null) {
            try {
                $this->streamCall->cancel();
            } catch (\Throwable $e) {
                Logger::warn("Error closing stream, clientId={$this->clientId}, error={$e->getMessage()}");
            }
            $this->streamCall = null;
        }

        Logger::info("Telemetry session stopped, clientId={$this->clientId}");
    }

    public function isActive(): bool {
        return $this->active;
    }

    /**
     * Set settings update callback
     * 
     * @param callable $callback Callback function(Settings $settings): void
     */
    public function setSettingsCallback(callable $callback): self {
        $this->settingsCallback = $callback;
        return $this;
    }

    /**
     * Set orphaned transaction command callback
     * 
     * @param callable $callback Callback function($command): void
     */
    public function setOrphanedTransactionCallback(callable $callback): self {
        $this->orphanedTransactionCallback = $callback;
        return $this;
    }

    /**
     * Send telemetry command to server
     */
    public function send(TelemetryCommand $command): void {
        if (!$this->active || $this->streamCall === null) {
            throw new \RuntimeException("Telemetry session is not active");
        }

        try {
            $this->streamCall->write($command);
        } catch (\Throwable $e) {
            Logger::error("Failed to send telemetry command, clientId={$this->clientId}, error={$e->getMessage()}");
            throw $e;
        }
    }

    /**
     * Send settings to server (Java: syncSettings0)
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
     * Send initial handshake message
     */
    private function sendHandshake(): void {
        $ua = new \Apache\Rocketmq\V2\UA();
        $ua->setLanguage(\Apache\Rocketmq\V2\Language::PHP);
        $ua->setVersion(self::getSdkVersion());

        $settings = new Settings();
        $settings->setClientType(ClientType::PRODUCER);
        $settings->setUserAgent($ua);

        $this->sendSettings($settings);
        Logger::debug("Handshake sent, clientId={$this->clientId}");
    }

    /**
     * Start background listener for incoming messages
     */
    private function startBackgroundListener(): void {
        if (extension_loaded('swoole') && class_exists('\\Swoole\\Coroutine')) {
            $this->startSwooleListener();
        } else {
            Logger::warn("Using simple listener mode (not async), clientId={$this->clientId}");
        }
    }

    /**
     * Start Swoole coroutine-based listener
     */
    private function startSwooleListener(): void {
        go(function() {
            Logger::debug("Started Swoole coroutine listener, clientId={$this->clientId}");

            while ($this->active && $this->streamCall !== null) {
                try {
                    $response = $this->streamCall->read();

                    if ($response === null) {
                        usleep(100000); // 100ms
                        continue;
                    }

                    if ($response instanceof TelemetryResponse) {
                        $this->handleResponse($response);
                    }
                } catch (\Throwable $e) {
                    Logger::error("Error in Swoole listener, clientId={$this->clientId}, error={$e->getMessage()}");

                    if ($this->reconnectAttempts < self::MAX_RECONNECT_ATTEMPTS) {
                        $this->attemptReconnect();
                    } else {
                        Logger::error("Max reconnect attempts reached, stopping session, clientId={$this->clientId}");
                        $this->stop();
                        break;
                    }
                }
            }
        });
    }

    /**
     * Handle incoming telemetry response
     * 
     * References Java ClientSessionImpl.onNext() command routing:
     * - SETTINGS: Apply server settings
     * - RECOVER_ORPHANED_TRANSACTION_COMMAND: Transaction recovery
     * - VERIFY_MESSAGE_COMMAND: Message verification
     * - PRINT_THREAD_STACK_TRACE_COMMAND: Debug stack traces
     */
    private function handleResponse(TelemetryResponse $response): void {
        Logger::debug("Received telemetry response, clientId={$this->clientId}");

        // Settings command
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

        // Recover orphaned transaction command
        if (method_exists($response, 'hasRecoverOrphanedTransactionCommand') &&
            $response->hasRecoverOrphanedTransactionCommand()) {
            Logger::info("Receive orphaned transaction recovery command from remote, clientId={$this->clientId}");
            if ($this->orphanedTransactionCallback !== null) {
                try {
                    call_user_func($this->orphanedTransactionCallback, $response->getRecoverOrphanedTransactionCommand());
                } catch (\Throwable $e) {
                    Logger::error("[Bug] Error in orphaned transaction callback, clientId={$this->clientId}, error={$e->getMessage()}");
                }
            }
            return;
        }

        // Verify message command
        if (method_exists($response, 'hasVerifyMessageCommand') &&
            $response->hasVerifyMessageCommand()) {
            Logger::info("Receive message verification command from remote, clientId={$this->clientId}");
            return;
        }

        Logger::warn("Receive unrecognized response from remote, clientId={$this->clientId}");
    }

    /**
     * Attempt to reconnect with exponential backoff + jitter
     * 
     * References Java ClientSessionImpl.renewRequestObserver():
     * - Uses scheduled delay for reconnection
     * - Re-syncs settings after successful reconnect
     * 
     * Jitter formula: delay = baseDelay * 2^(attempt-1) + random(0, baseDelay)
     * This prevents thundering herd when multiple clients reconnect simultaneously.
     */
    private function attemptReconnect(): void {
        $this->reconnectAttempts++;

        // Exponential backoff with jitter (Java pattern)
        $exponentialDelay = (int)(self::RECONNECT_BASE_DELAY_MS * pow(2, $this->reconnectAttempts - 1));
        $jitter = random_int(0, self::RECONNECT_BASE_DELAY_MS);
        $delayMs = min($exponentialDelay + $jitter, self::RECONNECT_MAX_DELAY_MS);

        Logger::warn("Attempting reconnection, attempt={$this->reconnectAttempts}, delay={$delayMs}ms, clientId={$this->clientId}");

        usleep($delayMs * 1000);

        try {
            $this->streamCall = $this->client->Telemetry([], [
                'timeout' => self::STREAM_TIMEOUT_SECONDS,
            ]);
            // Re-sync settings after successful reconnect (Java: syncSettings0())
            $this->sendHandshake();
            $this->reconnectAttempts = 0;
            Logger::info("Reconnection successful, clientId={$this->clientId}");
        } catch (\Throwable $e) {
            Logger::error("Reconnection failed, clientId={$this->clientId}, error={$e->getMessage()}");
        }
    }

    /**
     * Get SDK version from composer.json or fallback to constant
     */
    public static function getSdkVersion(): string {
        if (self::$sdkVersion !== null) {
            return self::$sdkVersion;
        }

        // Try reading from composer.json (similar to Java MetadataUtils.getVersion())
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
