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

use Apache\Rocketmq\SessionCredentials;
use Apache\Rocketmq\V2\Resource;


interface ConsumerInterface
{
    /**
     * Get the unique client identifier.
     * @return string
     */
    public function getClientId(): string;

    /**
     * Build a resource protobuf for the given topic name, with namespace if configured.
     * @param string $topic Topic name
     * @return Resource
     */
    public function getTopicResource(string $topic): Resource;

    /**
     * Get the resource namespace.
     * @return string
     */
    public function getNamespace(): string;

    /**
     * Build a Resource protobuf for the consumer group with namespace if configured.
     * @return Resource
     */
    public function getGroupResourceWithNamespace(): Resource;

    /**
     * Get session credentials for AK/SK authentication
     * @return SessionCredentials|null
     */
    public function getSessionCredentials(): ?SessionCredentials;

    /**
     * Build metadata for gRPC calls.
     * @param int|null $timeoutMs Optional timeout in milliseconds
     * @return array
     */
    public function buildMetadata(?int $timeoutMs = null): array;

    /**
     * ACK a message (acknowledge successful consumption)
     *
     * @param MessageView $messageView The message to
     * @return bool
     */
    public function ackMessage(MessageView $messageView): bool;

    /**
     * NACK a message (request redelivery after delay)
     * @param MessageView $messageView The message to negatively acknowledge
     * @param int $deliveryAttempt Current delivery attempt number
     * @param int|null $invisibleDuration Override invisible duration in seconds
     * @return bool
     */
    public function nackMessage(MessageView $messageView, int $deliveryAttempt = 1, ?int $invisibleDuration = null): bool;

    /**
     * Get the long-polling await duration int seconds.
     * @return int
     */
    public function getAwaitDuration(): int;

    /**
     * Get the number of messages to request per receive batch.
     * @return int
     */
    public function getReceiveBatchSize(): int;

    /**
     * get the max cached message count per ProcessQueue
     * @return int
     */
    public function getCacheMessageCountThresholdPerQueue(): int;

    /**
     * Get the max cached message bytes per ProcessQueue
     * @return int
     */
    public function getCacheMessageBytesThresholdPerQueue(): int;

    /**
     * Execute interceptor callbacks for a given hook point.
     * @param string $hookPoint MessageHookPoints constant (e.g., CONSUME, ACK)
     * @param array $context Context array with messageId, topic, success, etc.
     * @return void
     */
    public function executeInterceptors(string $hookPoint, array $context): void;

    /**
     * Get the retry policy for backoff calculation
     * @return \Apache\Rocketmq\ExponentialBackoffRetryPolicy|null
     */
    public function getRetryPolicy(): ?\Apache\Rocketmq\ExponentialBackoffRetryPolicy;

    /**
     * Get the consume service instance for ack/nack operations
     * @return \Apache\Rocketmq\ConsumeService|null
     */
    public function getConsumeService(): ?\Apache\Rocketmq\ConsumeService;
}