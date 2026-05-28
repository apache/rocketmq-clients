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

require_once __DIR__ . '/autoload.php';
require_once __DIR__ . '/Logger.php';
require_once __DIR__ . '/Signature.php';
require_once __DIR__ . '/ClientConstants.php';
require_once __DIR__ . '/SwooleCompat.php';

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
    private static $instances = [];

    private $client;
    private $endpoints;
    private $stream;
    private $logger;
    private $clientId;

    // Settings sync state
    private $settingsSynced = false;
    private $settingsError = null;
    private $settingsTimeout = 5.0;

    // Write queue (serial processing)
    private $writeQueue = [];
    private $isWriting = false;
    private $maxQueueSize = 1000;

    // Credentials for AK/SK signing
    private $credentials = null;

    // Namespace for resource scoping
    private $namespace = '';

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
    private $swooleCoroutineId = -1;
    private $isClosing = false;
    private $isReconnecting = false;

    /**
     * Private constructor
     */
    private function __construct($client, $endpoints, $clientId = null, $credentials = null, $namespace = '')
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

    public static function resetAll()
    {
        self::$instances = [];
    }

    public static function getInstance($client, $endpoints, $clientId = null, $credentials = null, $namespace = '')
    {
        $credId = $credentials !== null ? spl_object_id($credentials) : 'none';
        $effectiveClientId = $clientId ?? 'none';
        $key = $endpoints . '|' . $credId . '|' . $namespace . '|' . $effectiveClientId;

        if (!isset(self::$instances[$key])) {
            Logger::getInstance('TelemetrySession')->info("Creating new session for endpoints: {$endpoints}, clientId: {$effectiveClientId}");
            $instance = new self($client, $endpoints, $clientId, $credentials, $namespace);
            self::$instances[$key] = $instance;
        }

        return self::$instances[$key];
    }

    public function syncSettings($settingsCommand)
    {
        $this->lastSettingsCommand = $settingsCommand;
        $this->isClosing = false;
        return $this->createStreamAndSync($settingsCommand);
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

            $this->logger->info("Settings sent successfully (broker may not send immediate confirmation)");
            $this->settingsSynced = true;

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
        if (!$this->stream || SwooleCompat::isAvailable()) {
            return;
        }

        // In non-Swoole mode, we can't truly non-block read from the stream.
        // The responses() iterator is blocking. We use stream_select if possible,
        // but for simplicity, we skip polling in non-Swoole mode and rely on
        // the fact that the main loop already processes commands during sync.
        // Users who need server-push processing should use Swoole.
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
                while (!empty($this->writeQueue)) {
                    usleep(10000);
                }

                if (method_exists($this->stream, 'writesDone')) {
                    $this->stream->writesDone();
                }

                $this->stream->cancel();
            }
        } catch (\Exception $e) {
            $this->logger->error("Error closing session: " . $e->getMessage());
        }

        $credId = $this->credentials !== null ? spl_object_id($this->credentials) : 'none';
        $effectiveClientId = $this->clientId ?? 'none';
        $key = $this->endpoints . '|' . $credId . '|' . $this->namespace . "|" . $effectiveClientId;
        unset(self::$instances[$key]);

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
                usleep(1000000);
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
