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

namespace Apache\Rocketmq\Consumer;

use Apache\Rocketmq\Logger;

/**
 * FifoConsumeService - FIFO message consumption service.
 *
 * References Java FifoConsumeService:
 * - Processes messages in FIFO order within same message group
 * - Messages with same messageGroup are processed sequentially
 * - Different message groups can be processed in parallel
 * - Lock timeout forces lock release to prevent deadlocks
 */
class FifoConsumeService implements ConsumeService
{
    /** @var string */
    private string $clientId;

    /** @var callable */
    private $messageListener;

    /** @var PushConsumerImpl */
    private PushConsumerImpl $pushConsumer;

    /** @var bool */
    private bool $enableFifoConsumeAccelerator;

    /** @var array<string, float> Message group locks with acquisition timestamp */
    private array $messageGroupLocks = [];

    /** @var bool */
    private bool $running = false;

    /** @var int Lock timeout in seconds */
    private const LOCK_TIMEOUT_SECONDS = 30;

    /** @var int Lock spin wait interval in microseconds */
    private const LOCK_SPIN_INTERVAL_US = 10000; // 10ms

    public function __construct(
        string $clientId,
        callable $messageListener,
        PushConsumerImpl $pushConsumer,
        bool $enableFifoConsumeAccelerator = false
    ) {
        $this->clientId = $clientId;
        $this->messageListener = $messageListener;
        $this->pushConsumer = $pushConsumer;
        $this->enableFifoConsumeAccelerator = $enableFifoConsumeAccelerator;
    }

    public function start(): void
    {
        $this->running = true;
    }

    public function shutdown(): void
    {
        $this->running = false;
        $this->messageGroupLocks = [];
    }

    public function consume($message): ConsumeResult
    {
        if (!$this->running) {
            return ConsumeResult::FAILURE;
        }

        $messageGroup = $this->getMessageGroup($message);

        if (!$this->acquireMessageGroupLock($messageGroup)) {
            Logger::error("Failed to acquire FIFO message group lock after timeout, "
                . "messageGroup={$messageGroup}, clientId={$this->clientId}");
            return ConsumeResult::FAILURE;
        }

        try {
            $result = call_user_func($this->messageListener, $message);

            if ($result === ConsumeResult::SUCCESS || $result === true || $result === null) {
                return ConsumeResult::SUCCESS;
            }

            return ConsumeResult::FAILURE;
        } catch (\Throwable $e) {
            Logger::error("Exception while consuming FIFO message, "
                . "messageGroup={$messageGroup}, clientId={$this->clientId}, error=" . $e->getMessage());
            return ConsumeResult::FAILURE;
        } finally {
            $this->releaseMessageGroupLock($messageGroup);
        }
    }

    private function getMessageGroup($message): string
    {
        try {
            if (method_exists($message, 'getSystemProperties')) {
                $systemProperties = $message->getSystemProperties();
                if ($systemProperties !== null && method_exists($systemProperties, 'getMessageGroup')) {
                    $group = $systemProperties->getMessageGroup();
                    if ($group !== null && $group !== '') {
                        return $group;
                    }
                }
            }

            if (method_exists($message, 'getMessageGroup')) {
                $group = $message->getMessageGroup();
                if ($group !== null && $group !== '') {
                    return $group;
                }
            }
        } catch (\Throwable $e) {
            Logger::error("Failed to get message group, clientId={$this->clientId}, error=" . $e->getMessage());
        }

        return '__DEFAULT_GROUP__';
    }

    /**
     * Acquire lock for message group with timeout.
     *
     * If an existing lock has been held beyond LOCK_TIMEOUT_SECONDS, it is
     * considered stale and forcibly released to prevent deadlock.
     *
     * @return bool true if lock acquired, false on timeout
     */
    private function acquireMessageGroupLock(string $messageGroup): bool
    {
        $startTime = microtime(true);
        $deadline = $startTime + self::LOCK_TIMEOUT_SECONDS;

        while (isset($this->messageGroupLocks[$messageGroup])) {
            $now = microtime(true);

            // Check if the existing lock is stale (held beyond timeout)
            $lockAge = $now - $this->messageGroupLocks[$messageGroup];
            if ($lockAge > self::LOCK_TIMEOUT_SECONDS) {
                Logger::error("Force releasing stale FIFO lock, messageGroup={$messageGroup}, "
                    . "lockAge={$lockAge}s, clientId={$this->clientId}");
                unset($this->messageGroupLocks[$messageGroup]);
                break;
            }

            if ($now >= $deadline) {
                return false;
            }

            usleep(self::LOCK_SPIN_INTERVAL_US);
        }

        $this->messageGroupLocks[$messageGroup] = microtime(true);
        return true;
    }

    private function releaseMessageGroupLock(string $messageGroup): void
    {
        unset($this->messageGroupLocks[$messageGroup]);
    }

    public function isRunning(): bool
    {
        return $this->running;
    }

    public function isEnableFifoConsumeAccelerator(): bool
    {
        return $this->enableFifoConsumeAccelerator;
    }
}
