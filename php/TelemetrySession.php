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


use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\TelemetryCommand;
use Apache\Rocketmq\V2\Settings;
use Grpc\ChannelCredentials;

/**
 * TelemetrySession - Telemetry Session (full implementation referencing Java ClientSessionImpl)
 *
 * Core features:
 * 1. Singleton pattern (same Endpoints share Session)
 * 2. Settings sync confirmation mechanism
 * 3. Bidirectional stream management
 * 4. Command dispatch processing
 * 5. Automatic reconnection mechanism
 * 6. Swoole coroutine background reader for server-pushed commands
 */
class TelemetrySession
{
    private static array $instances = [];
    private static array $instanceTimestamps = [];
    private const MAX_INSTANCES = 10;
    private object $client;
    private string $endpoints;
    private $stream;
    private Logger $logger;
    private string $clientId;

    // Settings sync state
    private bool $settingsSynced = false;
    private ?string $settingsError = null;
    private float $settingsTimeout = 3.0; // seconds, matching Java's SETTINGS_INITIALIZATION_TIMEOUT

    // Credentials for AK/SK signing
    private ?SessionCredentials $credentials = null;

    // Namespace for resource scoping
    private string $namespace = '';

    // Settings received from server
    private $serverSettings = null;

    // Settings change callback
    private $onSettingsChange = null;

    // Server command callbacks
    private $onRecoverOrphanedTransaction = null;
    private $onVerifyMessage = null;
    private $onPrintThreadStackTrace = null;
    private $onReconnectEndpoints = null;
    private $onNotifyUnsubscribeLite = null;

    // Swoole coroutine reader state
    private int $swooleCoroutineId = -1;
    private bool $isClosing = false;
    private bool $isReconnecting = false;
    private $lastSettingsCommand = null;

    /**
     * Private constructor
     */
    private function __construct(object $client, string $endpoints, ?string $clientId = null, ?SessionCredentials $credentials = null, string $namespace = '')
    {
        $this->client = $client;
        $this->endpoints = $endpoints;
        $this->credentials = $credentials;
        $this->namespace = $namespace;
        $this->logger = Logger::getInstance('TelemetrySession');
        if ($clientId) {
            $this->clientId = $clientId;
        }
    }

    public function setOnSettingsChange(callable $callback): void
    {
        $this->onSettingsChange = $callback;
    }

    public function setOnRecoverOrphanedTransaction(callable $callback): void
    {
        $this->onRecoverOrphanedTransaction = $callback;
    }

    public function setOnVerifyMessage(callable $callback): void
    {
        $this->onVerifyMessage = $callback;
    }

    public function setOnPrintThreadStackTrace(callable $callback): void
    {
        $this->onPrintThreadStackTrace = $callback;
    }

    public function setOnReconnectEndpoints(callable $callback): void
    {
        $this->onReconnectEndpoints = $callback;
    }

    public function setOnNotifyUnsubscribeLite(callable $callback): void
    {
        $this->onNotifyUnsubscribeLite = $callback;
    }

    public function getServerSettings()
    {
        return $this->serverSettings;
    }

    public static function resetAll(): void
    {
        self::$instances = [];
        self::$instanceTimestamps = [];
    }

    public static function getInstance(object $client, string $endpoints, ?string $clientId = null, ?SessionCredentials $credentials = null, string $namespace = ''): self
    {
        $credId = $credentials !== null ? spl_object_id($credentials) : 'none';
        $effectiveClientId = $clientId ?? 'none';
        $key = $endpoints . '|' . $credId . '|' . $namespace . '|' . $effectiveClientId;

        if (isset(self::$instances[$key])) {
            $existing = self::$instances[$key];
            if (!$existing->isAlive()) {
                Logger::getInstance('TelemetrySession')->info("Evicting stale session for endpoints: {$endpoints}");
                unset(self::$instances[$key]);
                unset(self::$instanceTimestamps[$key]);
            }
        }

        if (!isset(self::$instances[$key])) {
            if (count(self::$instances) >= self::MAX_INSTANCES) {
                self::evictOldest();
            }
            Logger::getInstance('TelemetrySession')->info("Creating new session for endpoints: {$endpoints}, clientId: {$effectiveClientId}");
            $instance = new self($client, $endpoints, $clientId, $credentials, $namespace);
            self::$instances[$key] = $instance;
            self::$instanceTimestamps[$key] = time();
        }

        return self::$instances[$key];
    }

    /**
     * Check if this session is still olive (stream was created and is valid).
     * A session that never had a stream is not stale - it just not yet started
     */
    private function isAlive(): bool
    {
        if ($this->isClosing) {
            return false;
        }

        if ($this->stream !== null && $this->isStreamClosed()) {
            return false;
        }
        return true;
    }

    /**
     * Check if the underlying stream is closed.
     */
    private function isStreamClosed(): bool
    {
        if ($this->stream === null) {
            return true;
        }
        try {
            if (method_exists($this->stream, 'getStatus')) {
                $status = $this->stream->getStatus();
                $code = is_object($status) ? ($status->code ?? -1) : (is_array($status) ? ($status['code'] ?? -1) : -1);
                if ($code !== 0) {
                    return true;
                }
            }
        } catch (Exception $e) {
            return true;
        }
        return false;
    }

    /**
     * Evict the oldest instance to make room for a new one.
     * @return void
     */
    private static function evictOldest(): void
    {
        $oldestKey = null;
        $oldestTime = PHP_INT_MAX;
        foreach (self::$instanceTimestamps as $key => $timestamp) {
            if ($timestamp < $oldestTime) {
                $oldestTime = $timestamp;
                $oldestKey = $key;
            }
        }

        if ($oldestKey !== null) {
            Logger::getInstance('TelemetrySession')->info("Evicting oldest session (max instance reached): {$oldestKey}");
            if (isset(self::$instances[$oldestKey])) {
                self::$instances[$oldestKey]->close();
            }
            unset(self::$instances[$oldestKey]);
            unset(self::$instanceTimestamps[$oldestKey]);
        }
    }

    public function syncSettings($settingsCommand)
    {
        $this->lastSettingsCommand = $settingsCommand;
        $this->isClosing = false;
        
        // Create stream and send settings
        $success = $this->createStreamAndSync($settingsCommand);
        if (!$success) {
            return false;
        }
        
        // Wait for settings confirmation with timeout
        return $this->waitForSettingsConfirmation();
    }

    /**
     * Wait for settings confirmation from broker with timeout.
     * In Swoole mode, the background reader will set settingsSynced when SETTINGS is received.
     * In non-Swoole mode, we poll manually.
     */
    private function waitForSettingsConfirmation(): bool
    {
        $startTime = microtime(true);
        $pollInterval = 0.05; // 50ms
        
        while (microtime(true) - $startTime < $this->settingsTimeout) {
            if ($this->settingsSynced) {
                $elapsed = round(microtime(true) - $startTime, 2);
                $this->logger->info("Settings confirmed by broker after {$elapsed}s");
                return true;
            }
            
            if ($this->settingsError !== null) {
                $this->logger->error("Settings sync failed: " . $this->settingsError);
                return false;
            }
            
            // In non-Swoole mode, poll for responses
            if (!SwooleCompat::isAvailable()) {
                $this->pollTelemetryManual();
            }
            
            SwooleCompat::sleep((int)($pollInterval * 1000000));
        }
        
        // Timeout
        $this->settingsError = "Settings sync timeout after {$this->settingsTimeout}s";
        $this->logger->warning($this->settingsError);
        return false;
    }
    
    /**
     * Manual poll for telemetry responses (non-Swoole mode).
     * This is a blocking call that reads one response at a time.
     */
    private function pollTelemetryManual(): void
    {
        if (!$this->stream) {
            return;
        }
        
        try {
            // Try to read with a very short timeout
            // Note: gRPC PHP doesn't support non-blocking read easily,
            // so we just check if there's data available
            $response = $this->stream->read();
            if ($response !== null) {
                $this->handleResponse($response);
            }
        } catch (\Exception $e) {
            // Ignore read errors during polling
            $this->logger->debug("Poll read error: " . $e->getMessage());
        }
    }

    public function createStreamAndSync($settingsCommand)
    {
        try {
            $this->logger->info("Creating telemetry stream...");

            if (empty($this->namespace) && $settingsCommand->hasSettings()) {
                // Extract namespace from settings subscription group if not already set
                $settings = $settingsCommand->getSettings();
                if ($settings->hasSubscription()) {
                    $subscription = $settings->getSubscription();
                    if ($subscription->hasGroup()) {
                        $group = $subscription->getGroup();
                        if (method_exists($group, 'getResourceNamespace')) {
                            $ns = $group->getResourceNamespace();
                            if (!empty($ns)) {
                                $this->namespace = $ns;
                                $this->logger->info("Extracted namespace from settings command: {$ns}");
                            }
                        }
                    }
                }
            }

            $clientId = $this->clientId ?: $this->getClientIdFromCommand($settingsCommand);
            $metadata = Signature::sign(
                $this->credentials,
                $clientId,
                ClientConstants::LANGUAGE,
                ClientConstants::CLIENT_VERSION,
                $this->namespace,
                'v2'
            );

            $this->stream = $this->client->Telemetry($metadata);
            $this->logger->info("Stream created successfully");

            // Start background reader
            $this->startBackgroundReader();

            // Send Settings command
            $this->logger->info("Sending settings command...");
            $success = $this->writeSync($settingsCommand);

            if (!$success) {
                throw new \RuntimeException("Failed to send settings command");
            }

            $this->logger->info("Settings sent successfully, waiting for broker confirmation (timeout: {$this->settingsTimeout}s)...");
            // Don't set settingsSynced here - wait for confirmation from broker
            // The settingsSynced flag will be set in handleResponse() when we receive SETTINGS from broker

            return true;

        } catch (\Exception $e) {
            $this->logger->error("Failed to establish and sync settings: " . $e->getMessage());
            if (!$this->isClosing) {
                $this->scheduleReconnect($settingsCommand);
            }
            return false;
        }
    }

    /**
     * Start background reader. With Swoole, runs in a coroutine.
     * Without Swoole, the reader must be invoked manually via pollTelemetry().
     */
    private function startBackgroundReader()
    {
        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            $self = $this;
            \Swoole\Coroutine::create(function () use ($self) {
                $self->swooleCoroutineId = \Swoole\Coroutine::getCid();
                $self->logger->info("Swoole background reader started (coroutine ID: {$self->swooleCoroutineId})");
                $self->readResponsesInBackground();
                $self->swooleCoroutineId = -1;
                $self->logger->info("Swoole background reader stopped");
            });
        } else {
            $this->logger->info("Background reader will be invoked via pollTelemetry() in main loop");
        }
    }

    /**
     * Poll for telemetry responses (non-Swoole fallback).
     * Call this from the client's main loop to process server-pushed commands.
     * Note: This is a blocking call that reads one response at a time.
     */
    public function pollTelemetry(): void
    {
        if (!$this->stream) {
            return;
        }
        
        if (SwooleCompat::isAvailable()) {
            // In Swoole mode, background reader handles it
            return;
        }
        
        // In non-Swoole mode, manually poll
        $this->pollTelemetryManual();
    }

    private function readResponsesInBackground()
    {
        if (!$this->stream) {
            $this->logger->warning("No stream available for reading");
            return;
        }

        try {
            $this->logger->debug("Background reader started, listening for responses...");

            while (true) {
                if (!$this->stream) {
                    $this->logger->warning("Stream closed during background reading");
                    break;
                }

                $response = $this->stream->read();
                if ($response === null) {
                    $this->logger->debug("Background reader received null response");
                    break;
                }

                $this->handleResponse($response);
            }

            $this->logger->debug("Background reader finished");

        } catch (\Exception $e) {
            $this->logger->error("Error in background reader: " . $e->getMessage());

            if (!$this->settingsSynced) {
                $this->settingsError = $e->getMessage();
            }
        }
        if (!$this->isClosing && $this->lastSettingsCommand !== null) {
            $this->logger->warning("Telemetry stream lost, attempting reconnection");
            $this->scheduleReconnect($this->lastSettingsCommand);
        }
    }

    private function handleResponse($command)
    {
        $this->logger->info("Received command from broker");

        if ($command->hasSettings()) {
            $settings = $command->getSettings();
            $this->logger->info("Received SETTINGS command from broker");

            $this->serverSettings = $settings;

            if ($this->onSettingsChange !== null) {
                try {
                    ($this->onSettingsChange)($settings);
                } catch (\Exception $e) {
                    $this->logger->error("Settings change callback failed: " . $e->getMessage());
                }
            }

            $this->settingsSynced = true;

            if ($settings->hasClientType()) {
                $this->logger->debug("  ClientType: " . $settings->getClientType());
            }
        } elseif ($command->hasStatus()) {
            $status = $command->getStatus();
            $this->logger->info("Received STATUS command: Code=" . $status->getCode());
        } elseif ($command->hasRecoverOrphanedTransactionCommand()) {
            $recoverCmd = $command->getRecoverOrphanedTransactionCommand();
            $this->logger->info("Received RecoverOrphanedTransactionCommand: transactionId=" . $recoverCmd->getTransactionId());

            if ($this->onRecoverOrphanedTransaction !== null) {
                try {
                    ($this->onRecoverOrphanedTransaction)($recoverCmd);
                } catch (\Exception $e) {
                    $this->logger->error("RecoverOrphanedTransaction callback failed: " . $e->getMessage());
                }
            }
        } elseif ($command->hasVerifyMessageCommand()) {
            $verifyCmd = $command->getVerifyMessageCommand();
            $this->logger->info("Received VerifyMessageCommand: nonce=" . $verifyCmd->getNonce());

            if ($this->onVerifyMessage !== null) {
                try {
                    $response = ($this->onVerifyMessage)($verifyCmd);
                    if ($response instanceof TelemetryCommand) {
                        $this->writeSync($response);
                    }
                } catch (\Exception $e) {
                    $this->logger->error("VerifyMessage callback failed: " . $e->getMessage());
                }
            }
        } elseif ($command->hasPrintThreadStackTraceCommand()) {
            $printCmd = $command->getPrintThreadStackTraceCommand();
            $this->logger->info("Received PrintThreadStackTraceCommand: nonce=" . $printCmd->getNonce());

            if ($this->onPrintThreadStackTrace !== null) {
                try {
                    $response = ($this->onPrintThreadStackTrace)($printCmd);
                    if ($response instanceof TelemetryCommand) {
                        $this->writeSync($response);
                    }
                } catch (\Exception $e) {
                    $this->logger->error("PrintThreadStackTrace callback failed: " . $e->getMessage());
                }
            }
        } elseif ($command->hasReconnectEndpointsCommand()) {
            $reconnectCmd = $command->getReconnectEndpointsCommand();
            $this->logger->info("Received ReconnectEndpointsCommand: nonce=" . $reconnectCmd->getNonce());

            if ($this->onReconnectEndpoints !== null) {
                try {
                    ($this->onReconnectEndpoints)($reconnectCmd);
                } catch (\Exception $e) {
                    $this->logger->error("ReconnectEndpoints callback failed: " . $e->getMessage());
                }
            }
        } elseif ($command->hasNotifyUnsubscribeLiteCommand()) {
            $notifyCmd = $command->getNotifyUnsubscribeLiteCommand();
            $this->logger->info("Received NotifyUnsubscribeLiteCommand: liteTopic=" . $notifyCmd->getLiteTopic());

            if ($this->onNotifyUnsubscribeLite !== null) {
                try {
                    ($this->onNotifyUnsubscribeLite)($notifyCmd);
                } catch (\Exception $e) {
                    $this->logger->error("NotifyUnsubscribeLite callback failed: " . $e->getMessage());
                }
            }
        } else {
            $this->logger->debug("Received unrecognized command");
        }
    }

    public function writeSync($command)
    {
        try {
            if (!$this->stream) {
                $this->logger->error("Stream not initialized");
                return false;
            }

            $serialized = $command->serializeToString();
            if ($serialized === false || strlen($serialized) === 0) {
                $this->logger->error("Serialization failed");
                return false;
            }

            $result = $this->stream->write($command);

            if ($result === false) {
                $this->logger->error("write() returned false");
                return false;
            }

            if (method_exists($this->stream, 'flush')) {
                $this->stream->flush();
            }

            return true;

        } catch (\Exception $e) {
            $this->logger->error("writeSync failed: " . $e->getMessage());
            return false;
        }
    }

    public function close()
    {
        $this->logger->info("Closing session...");

        try {
            if ($this->stream) {
                // Signal that we're done writing
                if (method_exists($this->stream, 'writesDone')) {
                    $this->stream->writesDone();
                }

                // Cancel the stream
                $this->stream->cancel();
            }
        } catch (\Exception $e) {
            $this->logger->error("Error closing session: " . $e->getMessage());
        }

        $credId = $this->credentials !== null ? spl_object_id($this->credentials) : 'none';
        $effectiveClientId = $this->clientId ?? 'none';
        $key = $this->endpoints . '|' . $credId . '|' . $this->namespace . "|" . $effectiveClientId;
        unset(self::$instances[$key]);
        unset(self::$instanceTimestamps[$key]);

        $this->logger->info("Session closed");
    }

    public function getClientId()
    {
        return $this->clientId;
    }

    public function isSettingsSynced()
    {
        return $this->settingsSynced;
    }

    public function getSettingsError()
    {
        return $this->settingsError;
    }

    private function getClientIdFromCommand($command)
    {
        return 'php-client-' . getmypid() . '-' . time();
    }

    private function scheduleReconnect($settingsCommand)
    {
        if ($this->isClosing || $this->isReconnecting) {
            $this->logger->debug("Skipping telemetry reconnection : closing=" . $this->isClosing . ", reconnecting=" . $this->isReconnecting);
            return;
        }
        $this->isReconnecting = true;
        $this->logger->info("Scheduling telemetry reconnection in 1 second..");
        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            $self = $this;
            \Swoole\Coroutine::create(function () use ($self, $settingsCommand) {
                \Swoole\Coroutine::sleep(1);
                try {
                    if (!$self->isClosing) {
                        $self->logger->info("Reconnecting to telemetry..");
                        $self->createStreamAndSync($settingsCommand);
                    }
                } finally {
                    $self->isReconnecting = false;
                }
            });
        } else {
            try {
                SwooleCompat::sleep(1000000);
                if (!$this->isClosing) {
                    $this->logger->info("Reconnecting to telemetry..");
                    $this->createStreamAndSync($settingsCommand);
                }
            } finally {
                $this->isReconnecting = false;
            }
        }
    }
}
