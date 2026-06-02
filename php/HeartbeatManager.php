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

use Apache\Rocketmq\V2\HeartbeatRequest;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\NotifyClientTerminationRequest;
use Apache\Rocketmq\V2\ClientType;

/**
 * Manages periodic heartbeat and client termination notification for Producer.
 *
 * Depends on PublishingRouteManager for route data and ClientTraitProvider for
 * metadata/timeout helpers, rather than a raw Producer reference.
 */
class HeartbeatManager
{
    private int $timerId = -1;
    private int $lastHeartbeatTime = 0;
    private bool $inProgress = false;
    private readonly Logger $logger;

    public function __construct(
        private readonly PublishingRouteManager $routeManager,
        private readonly MessagingServiceClient $client,
        private readonly ClientTraitProvider $traitProvider,
        private readonly ?TlsCredentials $tlsCredentials = null,
    ) {
        $this->logger = Logger::getInstance('HeartbeatManager');
    }

    public function isInProgress(): bool
    {
        return $this->inProgress;
    }

    public function getLastHeartbeatTime(): int
    {
        return $this->lastHeartbeatTime;
    }

    /**
     * Start periodic heartbeat to all route endpoints.
     */
    public function start(): void
    {
        $this->doHeartbeat();
        $this->lastHeartbeatTime = time();

        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            $self = $this;
            $this->timerId = SwooleCompat::tick(10000, function () use ($self) {
                $self->onTick();
            });
            if ($this->timerId > 0) {
                $this->logger->debug("Started heartbeat with Swoole timer, timerId={$this->timerId}");
                return;
            }
        }
        if (function_exists('pcntl_signal') && function_exists('pcntl_alarm')) {
            $self = $this;
            pcntl_signal(SIGALRM, function () use ($self) {
                $self->onTick();
                pcntl_alarm(10);
            });
            pcntl_alarm(10);
            $this->logger->debug("Started heartbeat with PCNTL alarm");
        }
    }

    /**
     * Stop the periodic heartbeat and cancel pending alarm signals.
     */
    public function stop(): void
    {
        if ($this->timerId > 0) {
            SwooleCompat::clearTimer($this->timerId);
            $this->timerId = -1;
            $this->logger->debug("Swoole heartbeat timer cleared");
        }
        if (function_exists('pcntl_alarm')) {
            pcntl_alarm(0);
        }
        if (function_exists('pcntl_signal')) {
            pcntl_signal(SIGALRM, SIG_DFL);
        }

        $waitCount = 0;
        while ($this->inProgress && $waitCount < 10) {
            SwooleCompat::sleep(10000);
            $waitCount++;
        }

        if ($this->inProgress) {
            $this->logger->warning("Heartbeat still in progress after waiting, forcing shutdown");
        } else {
            $this->logger->debug("Heartbeat timer stopped cleanly");
        }
    }

    /**
     * Heartbeat tick handler, invoked by alarm signal or Swoole timer.
     */
    public function onTick(): void
    {
        $now = time();
        if ($now - $this->lastHeartbeatTime >= 10) {
            if ($this->inProgress) {
                $this->logger->debug("Heartbeat already in progress, skipping this tick");
                return;
            }

            $this->inProgress = true;
            try {
                $this->doHeartbeat();
                $this->lastHeartbeatTime = time();

                static $lastRouteRefresh = 0;
                if (time() - $lastRouteRefresh >= 30) {
                    $this->routeManager->refreshRouteCache();
                    $lastRouteRefresh = time();
                }
            } catch (\Throwable $e) {
                $this->logger->warning("Heartbeat tick failed: " . $e->getMessage());
            } finally {
                $this->inProgress = false;
            }
        }
    }

    /**
     * Send a heartbeat to all broker endpoints in the route cache.
     */
    public function doHeartbeat(): void
    {
        $routeCache = $this->routeManager->getRouteCache();
        if (empty($routeCache)) {
            return;
        }

        $brokerEndpoints = $this->routeManager->getTotalRouteEndpoints();
        if (empty($brokerEndpoints)) {
            return;
        }

        $request = new HeartbeatRequest();
        $request->setClientType(ClientType::PRODUCER);

        foreach ($brokerEndpoints as $endpoints) {
            $addresses = $endpoints->getAddresses();
            if (empty($addresses) || $addresses[0] === null) {
                continue;
            }
            $address = $addresses[0];
            $brokerKey = $address->getHost() . ':' . $address->getPort();
            try {
                $brokerClient = RpcClientManager::getInstance()->getClient($brokerKey, [
                    'tlsCredentials' => $this->tlsCredentials,
                ]);

                $heartbeatTimeoutMs = (int)($this->traitProvider->getOperationTimeout('HEARTBEAT') / 1000);
                $metadata = $this->traitProvider->buildMetadata($heartbeatTimeoutMs);
                $callOptions = ['timeout' => $this->traitProvider->getOperationTimeout('HEARTBEAT')];

                list($response, $status) = $brokerClient->Heartbeat($request, $metadata, $callOptions)->wait();
                if ($status->code === 0) {
                    $this->logger->debug("Heartbeat to broker {$brokerKey} successful");
                    $this->routeManager->clearIsolatedEndpoints();
                } else {
                    $this->logger->warning("Heartbeat to broker {$brokerKey} failed:" . $status->details);
                }
            } catch (\Exception $e) {
                $this->logger->warning("Heartbeat to broker {$brokerKey} failed:" . $e->getMessage());
            }
        }
    }

    /**
     * Notify the server that this client is terminating.
     */
    public function notifyClientTermination(): void
    {
        $routeCache = $this->routeManager->getRouteCache();
        if (empty($routeCache)) {
            return;
        }

        $request = new NotifyClientTerminationRequest();

        $timeoutMs = (int)($this->traitProvider->getOperationTimeout('HEARTBEAT') / 1000);
        $metadata = $this->traitProvider->buildMetadata($timeoutMs);
        $callOptions = ['timeout' => $this->traitProvider->getOperationTimeout('HEARTBEAT')];

        try {
            list($response, $status) = $this->client->NotifyClientTermination($request, $metadata, $callOptions)->wait();
            if ($status->code === 0) {
                $this->logger->debug("NotifyClientTermination sent successfully");
            } else {
                $this->logger->warning("NotifyClientTermination failed: " . $status->details);
            }
        } catch (\Exception $e) {
            $this->logger->warning("NotifyClientTermination exception: " . $e->getMessage());
        }
    }
}
