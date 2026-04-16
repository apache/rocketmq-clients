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
 * StandardConsumeService - Standard message consumption service.
 *
 * References Java StandardConsumeService:
 * - Processes messages without ordering guarantee
 * - Catches Throwable (not just Exception) for robust error isolation
 * - Uses Logger instead of error_log for unified logging
 */
class StandardConsumeService implements ConsumeService
{
    /** @var string */
    private string $clientId;

    /** @var callable */
    private $messageListener;

    /** @var PushConsumerImpl */
    private PushConsumerImpl $pushConsumer;

    /** @var bool */
    private bool $running = false;

    public function __construct(
        string $clientId,
        callable $messageListener,
        PushConsumerImpl $pushConsumer
    ) {
        $this->clientId = $clientId;
        $this->messageListener = $messageListener;
        $this->pushConsumer = $pushConsumer;
    }

    public function start(): void
    {
        $this->running = true;
    }

    public function shutdown(): void
    {
        $this->running = false;
    }

    public function consume($message): ConsumeResult
    {
        if (!$this->running) {
            return ConsumeResult::FAILURE;
        }

        try {
            $result = call_user_func($this->messageListener, $message);

            if ($result === ConsumeResult::SUCCESS || $result === true || $result === null) {
                return ConsumeResult::SUCCESS;
            }

            return ConsumeResult::FAILURE;
        } catch (\Throwable $e) {
            Logger::error("Exception while consuming message, clientId={$this->clientId}, error=" . $e->getMessage());
            return ConsumeResult::FAILURE;
        }
    }

    public function isRunning(): bool
    {
        return $this->running;
    }
}
