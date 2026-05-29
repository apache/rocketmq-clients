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

class ClientMetrics
{
    private static $instance = null;

    private $sendCount = 0;
    private $sendErrorCount = 0;
    private $sendLatencyMs = [];
    private $receiveCount = 0;
    private $receiveErrorCount = 0;
    private $consumeOkCount = 0;
    private $consumeErrorCount = 0;
    private $ackCount = 0;
    private $ackErrorCount = 0;
    private $startTime = 0;

    /**
     * Private constructor - initialize start time.
     */
    private function __construct()
    {
        $this->startTime = time();
    }

    /**
     * Get the singleton instance of ClientMetrics.
     *
     * @return ClientMetrics The singleton ClientMetrics instance
     */
    public static function getInstance()
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    /**
     * Reset the singleton instance to null.
     *
     * @return void
     */
    public static function reset()
    {
        self::$instance = null;
    }

    /**
     * Record a send operation with success flag and latency.
     *
     * @param bool $success Whether the send operation succeeded
     * @param int  $latencyMs Send latency in milliseconds
     * @return void
     */
    public function recordSend(bool $success = true, int $latencyMs = 0): void
    {
        $this->sendCount++;
        if (!$success) {
            $this->sendErrorCount++;
        }
        $this->sendLatencyMs[] = $latencyMs;
        // Keep only last 1000 entries
        if (count($this->sendLatencyMs) > 1000) {
            $this->sendLatencyMs = array_slice($this->sendLatencyMs, -500);
        }
    }

    /**
     * Record a receive operation with success flag.
     *
     * @param bool $success Whether the receive operation succeeded
     * @return void
     */
    public function recordReceive(bool $success = true): void
    {
        $this->receiveCount++;
        if (!$success) {
            $this->receiveErrorCount++;
        }
    }

    /**
     * Record a consume operation with success flag.
     *
     * @param bool $success Whether the consume operation succeeded
     * @return void
     */
    public function recordConsume(bool $success = true): void
    {
        if ($success) {
            $this->consumeOkCount++;
        } else {
            $this->consumeErrorCount++;
        }
    }

    /**
     * Record an ack operation with success flag.
     *
     * @param bool $success Whether the ack operation succeeded
     * @return void
     */
    public function recordAck(bool $success = true): void
    {
        $this->ackCount++;
        if (!$success) {
            $this->ackErrorCount++;
        }
    }

    /**
     * Get total send count.
     *
     * @return int Total number of send operations recorded
     */
    public function getSendCount(): int { return $this->sendCount; }

    /**
     * Get send error count.
     *
     * @return int Total number of failed send operations
     */
    public function getSendErrorCount(): int { return $this->sendErrorCount; }

    /**
     * Get total receive count.
     *
     * @return int Total number of receive operations recorded
     */
    public function getReceiveCount(): int { return $this->receiveCount; }

    /**
     * Get receive error count.
     *
     * @return int Total number of failed receive operations
     */
    public function getReceiveErrorCount(): int { return $this->receiveErrorCount; }

    /**
     * Get successful consume count.
     *
     * @return int Total number of successful consume operations
     */
    public function getConsumeOkCount(): int { return $this->consumeOkCount; }

    /**
     * Get consume error count.
     *
     * @return int Total number of failed consume operations
     */
    public function getConsumeErrorCount(): int { return $this->consumeErrorCount; }

    /**
     * Get total ack count.
     *
     * @return int Total number of ack operations recorded
     */
    public function getAckCount(): int { return $this->ackCount; }

    /**
     * Get ack error count.
     *
     * @return int Total number of failed ack operations
     */
    public function getAckErrorCount(): int { return $this->ackErrorCount; }

    /**
     * Get the average send latency in milliseconds.
     *
     * @return float Average send latency across all recorded send operations
     */
    public function getAverageSendLatencyMs(): float
    {
        if (empty($this->sendLatencyMs)) {
            return 0.0;
        }
        return array_sum($this->sendLatencyMs) / count($this->sendLatencyMs);
    }

    /**
     * Get the uptime in seconds since instance creation.
     *
     * @return int Number of seconds elapsed since the ClientMetrics instance was created
     */
    public function getUptimeSeconds(): int
    {
        return time() - $this->startTime;
    }

    /**
     * Get a snapshot of all metrics statistics.
     *
     * @return array Associative array containing all current metrics values
     */
    public function getStats(): array
    {
        return [
            'uptimeSeconds' => $this->getUptimeSeconds(),
            'sendCount' => $this->sendCount,
            'sendErrorCount' => $this->sendErrorCount,
            'avgSendLatencyMs' => round($this->getAverageSendLatencyMs(), 2),
            'receiveCount' => $this->receiveCount,
            'receiveErrorCount' => $this->receiveErrorCount,
            'consumeOkCount' => $this->consumeOkCount,
            'consumeErrorCount' => $this->consumeErrorCount,
            'ackCount' => $this->ackCount,
            'ackErrorCount' => $this->ackErrorCount,
        ];
    }
}

class MetricsInterceptor implements MessageInterceptor
{
    private $metrics;

    /**
     * Construct a metrics interceptor and attach the singleton ClientMetrics.
     */
    public function __construct()
    {
        $this->metrics = ClientMetrics::getInstance();
    }

    /**
     * Intercept a message hook point and record the corresponding metric.
     *
     * @param string $hookPoint The hook point identifier (one of MessageHookPoints constants)
     * @param array  $context Context data including success flag and optional latency
     * @return void
     */
    public function intercept(string $hookPoint, array $context = []): void
    {
        switch ($hookPoint) {
            case MessageHookPoints::SEND:
                $success = $context['success'] ?? true;
                $latencyMs = $context['latencyMs'] ?? 0;
                $this->metrics->recordSend($success, $latencyMs);
                break;
            case MessageHookPoints::RECEIVE:
                $success = $context['success'] ?? true;
                $this->metrics->recordReceive($success);
                break;
            case MessageHookPoints::CONSUME:
                $success = $context['success'] ?? true;
                $this->metrics->recordConsume($success);
                break;
            case MessageHookPoints::ACK:
                $success = $context['success'] ?? true;
                $this->metrics->recordAck($success);
                break;
        }
    }

    /**
     * Get the ClientMetrics instance attached to this interceptor.
     *
     * @return ClientMetrics The singleton metrics instance used by this interceptor
     */
    public function getMetrics(): ClientMetrics
    {
        return $this->metrics;
    }
}
