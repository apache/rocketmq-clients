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
use Apache\Rocketmq\V2\Settings as V2Settings;
use Apache\Rocketmq\V2\Subscription;
use Apache\Rocketmq\V2\SubscriptionEntry;
use Apache\Rocketmq\V2\Resource;
use Apache\Rocketmq\V2\FilterExpression as V2FilterExpression;
use Apache\Rocketmq\V2\FilterType;
use Google\Protobuf\Duration;

/**
 * Push subscription settings for PushConsumer.
 *
 * References Java PushSubscriptionSettings:
 * - sync() applies server-returned retry policy
 * - toProtobuf() serializes subscriptions with proper FilterExpression type/expression
 */
class PushSubscriptionSettings extends ClientSettings {

    /** @var string Consumer group */
    private string $consumerGroup;

    /** @var array<string, mixed> Topic => FilterExpression map */
    private array $subscriptionExpressions;

    /** @var int Max cache message count */
    private int $maxCacheMessageCount = 1000;

    /** @var int Max cache message size in bytes */
    private int $maxCacheMessageSize = 67108864; // 64MB

    /** @var int Consumption thread count */
    private int $consumptionThreadCount = 20;

    /** @var Duration|null Long polling timeout */
    private ?Duration $longPollingTimeout = null;

    public function __construct(
        string $namespace,
        string $clientId,
        string $endpoints,
        string $consumerGroup,
        array $subscriptionExpressions = [],
        int $maxAttempts = 3,
        int $requestTimeoutMs = 3000
    ) {
        parent::__construct(
            $namespace,
            $clientId,
            ClientType::PUSH_CONSUMER,
            $endpoints,
            $maxAttempts,
            $requestTimeoutMs
        );

        $this->consumerGroup = $consumerGroup;
        $this->subscriptionExpressions = $subscriptionExpressions;
    }

    /**
     * {@inheritdoc}
     */
    public function toProtobuf(): V2Settings {
        $settings = new V2Settings();
        $settings->setAccessPoint($this->endpoints);
        $settings->setClientType(ClientType::PUSH_CONSUMER);
        $settings->setRequestTimeout($this->requestTimeout);

        $subscription = new Subscription();

        $groupResource = new Resource();
        $groupResource->setName($this->consumerGroup);
        $groupResource->setResourceNamespace($this->namespace);
        $subscription->setGroup($groupResource);

        foreach ($this->subscriptionExpressions as $topic => $filterExpression) {
            $topicResource = new Resource();
            $topicResource->setName($topic);
            $topicResource->setResourceNamespace($this->namespace);

            $entry = new SubscriptionEntry();
            $entry->setTopic($topicResource);

            $v2Filter = new V2FilterExpression();
            if (is_object($filterExpression) && method_exists($filterExpression, 'getExpression')) {
                $v2Filter->setExpression($filterExpression->getExpression());
                if (method_exists($filterExpression, 'getType')) {
                    $type = $filterExpression->getType();
                    $v2Filter->setType($type === 'SQL92' ? FilterType::SQL : FilterType::TAG);
                } else {
                    $v2Filter->setType(FilterType::TAG);
                }
            } elseif (is_string($filterExpression)) {
                $v2Filter->setExpression($filterExpression);
                $v2Filter->setType(FilterType::TAG);
            }

            $entry->setExpression($v2Filter);
            $subscription->getSubscriptions()[] = $entry;
        }

        $settings->setSubscription($subscription);

        if ($this->retryPolicy !== null) {
            $backoffPolicy = new \Apache\Rocketmq\V2\RetryPolicy();
            $backoffPolicy->setMaxAttempts($this->retryPolicy->getMaxAttempts());
            $settings->setBackoffPolicy($backoffPolicy);
        }

        return $settings;
    }

    /**
     * Sync settings from server-returned protobuf.
     *
     * Applies: retryPolicy (maxAttempts, exponentialBackoff).
     */
    public function sync(V2Settings $settings): void {
        if (!$settings->hasSubscription()) {
            Logger::error("[Bug] Unexpected pubSub case in subscription settings sync, clientId={$this->clientId}");
            return;
        }

        // Sync retry policy
        if ($settings->hasBackoffPolicy()) {
            $backoffPolicy = $settings->getBackoffPolicy();
            $maxAttempts = $backoffPolicy->getMaxAttempts();

            if ($backoffPolicy->hasExponentialBackoff()) {
                $exponential = $backoffPolicy->getExponentialBackoff();
                $initialMs = 1000;
                $maxMs = 60000;
                $multiplier = 2.0;

                if ($exponential->hasInitial()) {
                    $initialMs = (int)($exponential->getInitial()->getSeconds() * 1000
                        + $exponential->getInitial()->getNanos() / 1000000);
                }
                if ($exponential->hasMax()) {
                    $maxMs = (int)($exponential->getMax()->getSeconds() * 1000
                        + $exponential->getMax()->getNanos() / 1000000);
                }
                $multiplier = $exponential->getMultiplier() > 0 ? $exponential->getMultiplier() : 2.0;

                $this->retryPolicy = new ExponentialBackoffRetryPolicy(
                    $maxAttempts > 0 ? $maxAttempts : $this->maxAttempts,
                    $initialMs,
                    $maxMs,
                    $multiplier
                );
            }

            if ($maxAttempts > 0) {
                $this->maxAttempts = $maxAttempts;
            }
        }
    }

    public function getConsumerGroup(): string {
        return $this->consumerGroup;
    }

    public function getSubscriptionExpressions(): array {
        return $this->subscriptionExpressions;
    }

    public function addSubscription(string $topic, $filterExpression): self {
        $this->subscriptionExpressions[$topic] = $filterExpression;
        return $this;
    }

    public function getMaxCacheMessageCount(): int {
        return $this->maxCacheMessageCount;
    }

    public function setMaxCacheMessageCount(int $count): self {
        $this->maxCacheMessageCount = $count;
        return $this;
    }

    public function getMaxCacheMessageSize(): int {
        return $this->maxCacheMessageSize;
    }

    public function setMaxCacheMessageSize(int $size): self {
        $this->maxCacheMessageSize = $size;
        return $this;
    }

    public function getConsumptionThreadCount(): int {
        return $this->consumptionThreadCount;
    }

    public function setConsumptionThreadCount(int $count): self {
        $this->consumptionThreadCount = $count;
        return $this;
    }

    public function getLongPollingTimeout(): ?Duration {
        return $this->longPollingTimeout;
    }

    public function setLongPollingTimeout(Duration $timeout): self {
        $this->longPollingTimeout = $timeout;
        return $this;
    }
}
