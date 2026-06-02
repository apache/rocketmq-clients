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
class FakeConsumer implements \Apache\Rocketmq\ConsumerInterface
{
    public int $countThreshold = 1024;
    public int $bytesThreshold = 1048576;
    public int $awaitDuration = 30;
    public int $receiveBathSize = 32;
    public string $clientId;
    public array $ackCalls = [];
    public array $nackCalls = [];

    public function __construct(string $clientId = 'test-consumer')
    {
        $this->clientId = $clientId;
    }

    public function getClientId(): string
    {
        return $this->clientId;
    }

    public function getTopicResource(string $topic): \Apache\Rocketmq\V2\Resource
    {
        $r = new \Apache\Rocketmq\V2\Resource();
        $r->setName($topic);
        return $r;
    }

    public function getNamespace(): string
    {
        return  '';
    }

    public function getGroupResourceWithNamespace(): \Apache\Rocketmq\V2\Resource
    {
        $r = new \Apache\Rocketmq\V2\Resource();
        $r->setName('test-group');
        return $r;
    }

    public function getSessionCredentials(): ?\Apache\Rocketmq\SessionCredentials
    {
        return null;
    }

    public function ackMessage(\Apache\Rocketmq\MessageView $messageView): bool
    {
        $this->ackCalls[] = $messageView;
        return true;
    }

    public function executeInterceptors(string $hookPoint, array $context): void
    {
    }

    public function getRetryPolicy(): ?\Apache\Rocketmq\ExponentialBackoffRetryPolicy
    {
        return null;
    }

    public function getConsumeService(): ?\Apache\Rocketmq\ConsumeService
    {
        return null;
    }

    public function nackMessage(\Apache\Rocketmq\MessageView $messageView, int $deliveryAttempt = 1, ?int $invisibleDuration = null): bool
    {
        $this->nackCalls[] = $messageView;
        return true;
    }

    public function getAwaitDuration(): int
    {
        return $this->awaitDuration;
    }

    public function getReceiveBatchSize(): int
    {
        return $this->receiveBathSize;
    }

    public function getCacheMessageCountThresholdPerQueue(): int
    {
        return $this->countThreshold;
    }

    public function getCacheMessageBytesThresholdPerQueue(): int
    {
        return $this->bytesThreshold;
    }

    public function getClient(): \Apache\Rocketmq\V2\MessagingServiceClient
    {
        throw new \RuntimeException('FakeConsumer::getClient() should not be called directly. Use GrpcMockHelper instead.');
    }
}
