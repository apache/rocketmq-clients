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
use Apache\Rocketmq\V2\RecallMessageRequest;
use Apache\Rocketmq\V2\Resource;

/**
 * RecallMessageHandler — Handles message recall operations.
 *
 * Extracted from Producer to separate recall concerns:
 * - recall(): synchronous recall via gRPC
 * - recallAsync(): Swoole coroutine-based async recall with Generator fallback
 *
 * Dependencies are injected; the handler has no lifecycle state of its own.
 * The Producer is responsible for running-state checks before delegating here.
 */
class RecallMessageHandler
{
    private readonly Logger $logger;

    public function __construct(
        private readonly MessagingServiceClient $client,
        private readonly ProducerSettings $settings,
        private readonly \Closure $metadataBuilder,
        private readonly \Closure $callOptionsResolver,
    ) {
        $this->logger = Logger::getInstance('Producer');
    }

    /**
     * Recall a previously sent message.
     *
     * @param string $topic Topic name
     * @param string $recallHandle Recall handle from send result
     * @return array{messageId: string, status: object} Recall result
     * @throws \RuntimeException If the gRPC call fails
     */
    public function recall(string $topic, string $recallHandle): array
    {
        $topicResource = new Resource();
        $topicResource->setName($topic);

        $request = new RecallMessageRequest();
        $request->setTopic($topicResource);
        $request->setRecallHandle($recallHandle);

        $metadata = ($this->metadataBuilder)($this->settings->getRequestTimeout());
        list($response, $status) = $this->client->RecallMessage(
            $request, $metadata, ($this->callOptionsResolver)()
        )->wait();

        if ($status->code !== 0) {
            throw new \RuntimeException("Recall message failed: " . $status->details);
        }

        return [
            'messageId' => $response->getMessageId() ?? '',
            'status' => $response->getStatus(),
        ];
    }

    /**
     * Recall a message asynchronously.
     *
     * Uses Swoole coroutine when available; falls back to Generator otherwise.
     *
     * @param string $topic Topic name
     * @param string $recallHandle Recall handle from send result
     * @return array|\Generator
     * @throws \RuntimeException On timeout or gRPC failure
     */
    public function recallAsync(string $topic, string $recallHandle): array|\Generator
    {
        if (SwooleCompat::isAvailable() && SwooleCompat::inCoroutine()) {
            $channel = new \Swoole\Coroutine\Channel(1);
            \Swoole\Coroutine::create(function () use ($topic, $recallHandle, $channel) {
                try {
                    $result = $this->recall($topic, $recallHandle);
                    $channel->push(['success' => true, 'result' => $result]);
                } catch (\Throwable $e) {
                    $channel->push(['success' => false, 'exception' => $e]);
                }
            });
            $data = $channel->pop($this->settings->getRequestTimeout() / 1000.0);
            if ($data === false) {
                throw new \RuntimeException(
                    "Recall message async Request timeout {$this->settings->getRequestTimeout()}ms"
                );
            }
            if (isset($data['exception'])) {
                throw $data['exception'];
            }
            return $data['result'] ?? null;
        }
        return $this->recallSyncFallback($topic, $recallHandle);
    }

    /**
     * Generator fallback for recallAsync when Swoole is not available.
     *
     * @param string $topic
     * @param string $recallHandle
     * @return \Generator
     */
    private function recallSyncFallback(string $topic, string $recallHandle): \Generator
    {
        yield $this->recall($topic, $recallHandle);
    }
}
