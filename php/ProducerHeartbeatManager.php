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
use Apache\Rocketmq\V2\HeartbeatRequest;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\NotifyClientTerminationRequest;
use Apache\Rocketmq\V2\TelemetryCommand;
use Grpc\ChannelCredentials;

/**
 * ProducerHeartbeatManager - Manages heartbeat and telemetry session lifecycle
 * 
 * Responsibilities:
 * 1. Start/stop periodic heartbeat via SIGALRM timer
 * 2. Establish and maintain TelemetrySession
 * 3. Notify client termination
 * 4. Handle settings changes from server
 */
class ProducerHeartbeatManager
{
    private MessagingServiceClient $client;
    private string $endpoints;
    private string $clientId;
    private ?SessionCredentials $credentials;
    private string $namespace;
    private Logger $logger;
    private TelemetrySession $telemetrySession;
    
    // Heartbeat state
    private $heartbeatPid = null;
    private int $lastHeartbeatTime = 0;
    private bool $heartbeatInProgress = false;
    private bool $isRunning = false;

    /**
     * @param MessagingServiceClient $client gRPC client
     * @param string $endpoints Server endpoints
     * @param string|null $clientId Client identifier (nullable for testing)
     * @param SessionCredentials|null $credentials AK/SK credentials
     * @param string $namespace Namespace
     * @param Logger|null $logger Logger instance (optional, will create if null)
     */
    public function __construct(
        MessagingServiceClient $client,
        string $endpoints,
        ?string $clientId,
        ?SessionCredentials $credentials,
        string $namespace = '',
        ?Logger $logger = null
    ) {
        $this->client = $client;
        $this->endpoints = $endpoints;
        $this->clientId = $clientId ?? ('php-producer-' . getmypid() . '-' . time());
        $this->credentials = $credentials;
        $this->namespace = $namespace;
        $this->logger = $logger ?? Logger::getInstance('Producer');
        
        // Initialize Telemetry Session (singleton)
        $this->telemetrySession = TelemetrySession::getInstance(
            $client, 
            $endpoints, 
            $clientId, 
            $credentials, 
            $namespace
        );
    }

    /**
     * Establish telemetry session and register callbacks
     *
     * @param callable|null $settingsCallback Callback for settings changes
     * @param callable|null $transactionCallback Callback for orphaned transactions
     */
    public function establishTelemetrySession(?callable $settingsCallback = null, ?callable $transactionCallback = null): void
    {
        try {
            $this->telemetrySession->establish();
            
            // Register settings change callback
            if ($settingsCallback !== null) {
                $this->telemetrySession->registerSettingsCallback($settingsCallback);
            }
            
            // Register transaction checker callback
            if ($transactionCallback !== null) {
                $this->telemetrySession->registerTransactionCallback($transactionCallback);
            }
            
            $this->logger->debug("Telemetry session established successfully");
        } catch (\Exception $e) {
            $this->logger->error("Failed to establish telemetry session: " . $e->getMessage());
            throw $e;
        }
    }

    /**
     * Start periodic heartbeat via SIGALRM timer
     */
    public function startHeartbeat(): void
    {
        $this->doHeartbeat();
        $this->lastHeartbeatTime = time();
        
        if (function_exists('pcntl_signal')) {
            $self = $this;
            pcntl_signal(SIGALRM, function () use ($self) {
                // Concurrency guard: prevent reentrant heartbeat
                if ($self->heartbeatInProgress) {
                    $self->logger->debug("SIGALRM received but heartbeat already in progress, skipping");
                    $self->scheduleNextHeartbeat();
                    return;
                }
                
                $self->onHeartbeatTick();
            });
            
            $this->scheduleNextHeartbeat();
            $this->logger->debug("Heartbeat timer started");
        } else {
            $this->logger->warning("pcntl extension not available, heartbeat disabled");
        }
    }

    /**
     * Stop heartbeat timer
     */
    public function stopHeartbeat(): void
    {
        if ($this->heartbeatPid !== null) {
            posix_kill($this->heartbeatPid, SIGKILL);
            pcntl_waitpid($this->heartbeatPid, $status);
            $this->heartbeatPid = null;
            $this->logger->debug("Heartbeat timer stopped");
        }
        
        // Wait for any in-progress heartbeat to complete
        $waitCount = 0;
        while ($this->heartbeatInProgress && $waitCount < 10) {
            SwooleCompat::sleep(10000); // Wait 10ms
            $waitCount++;
        }
    }

    /**
     * Notify server that this client is terminating
     */
    public function notifyClientTermination(): void
    {
        $request = new NotifyClientTerminationRequest();
        $groupResource = new Resource();
        $groupResource->setName($this->clientId);
        $request->setGroup($groupResource);

        try {
            list($response, $status) = $this->client->NotifyClientTermination(
                $request,
                $this->buildMetadata(),
                $this->getCallOptions()
            )->wait();
            
            if ($status->code === 0) {
                $this->logger->debug("NotifyClientTermination sent successfully");
            } else {
                $this->logger->warning("NotifyClientTermination failed: " . $status->details);
            }
        } catch (\Exception $e) {
            $this->logger->warning("NotifyClientTermination exception: " . $e->getMessage());
        }
    }

    /**
     * Check if heartbeat is currently in progress
     *
     * @return bool
     */
    public function isHeartbeatInProgress(): bool
    {
        return $this->heartbeatInProgress;
    }

    /**
     * Get last heartbeat timestamp
     *
     * @return int
     */
    public function getLastHeartbeatTime(): int
    {
        return $this->lastHeartbeatTime;
    }

    /**
     * Close telemetry session
     */
    public function closeTelemetrySession(): void
    {
        $this->telemetrySession->close();
    }

    /**
     * Set running state
     *
     * @param bool $running
     */
    public function setRunning(bool $running): void
    {
        $this->isRunning = $running;
    }

    /**
     * Check if manager is running
     *
     * @return bool
     */
    public function isRunning(): bool
    {
        return $this->isRunning;
    }

    /**
     * Schedule next heartbeat tick
     */
    private function scheduleNextHeartbeat(): void
    {
        if (function_exists('pcntl_alarm')) {
            pcntl_alarm(10); // Send SIGALRM every 10 seconds
        }
    }

    /**
     * Handle heartbeat tick
     */
    private function onHeartbeatTick(): void
    {
        $this->heartbeatInProgress = true;
        
        try {
            $this->doHeartbeat();
            $this->lastHeartbeatTime = time();
        } catch (\Exception $e) {
            $this->logger->error("Heartbeat failed: " . $e->getMessage());
        } finally {
            $this->heartbeatInProgress = false;
            $this->scheduleNextHeartbeat();
        }
    }

    /**
     * Execute heartbeat request
     */
    private function doHeartbeat(): void
    {
        $request = new HeartbeatRequest();
        $resource = new Resource();
        $resource->setName($this->clientId);
        $request->setClientType(ClientType::PRODUCER);
        
        try {
            list($response, $status) = $this->client->Heartbeat(
                $request,
                $this->buildMetadata(),
                $this->getCallOptions()
            )->wait();
            
            if ($status->code !== 0) {
                $this->logger->warning("Heartbeat failed: " . $status->details);
            } else {
                $this->logger->debug("Heartbeat sent successfully");
            }
        } catch (\Exception $e) {
            $this->logger->error("Heartbeat exception: " . $e->getMessage());
        }
    }

    /**
     * Build gRPC metadata
     *
     * @return array
     */
    private function buildMetadata(): array
    {
        $metadata = [];
        
        if ($this->credentials !== null) {
            $signature = Signature::sign($this->credentials, $this->clientId);
            $metadata['authorization'] = [$signature];
        }
        
        return $metadata;
    }

    /**
     * Get gRPC call options
     *
     * @return array
     */
    private function getCallOptions(): array
    {
        return [
            'timeout' => 3.0, // 3 seconds timeout
        ];
    }
}
