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

    private function __construct()
    {
        $this->startTime = time();
    }

    public static function getInstance()
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }

    public static function reset()
    {
        self::$instance = null;
    }

    public function recordSend($success = true, $latencyMs = 0)
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

    public function recordReceive($success = true)
    {
        $this->receiveCount++;
        if (!$success) {
            $this->receiveErrorCount++;
        }
    }

    public function recordConsume($success = true)
    {
        if ($success) {
            $this->consumeOkCount++;
        } else {
            $this->consumeErrorCount++;
        }
    }

    public function recordAck($success = true)
    {
        $this->ackCount++;
        if (!$success) {
            $this->ackErrorCount++;
        }
    }

    public function getSendCount() { return $this->sendCount; }
    public function getSendErrorCount() { return $this->sendErrorCount; }
    public function getReceiveCount() { return $this->receiveCount; }
    public function getReceiveErrorCount() { return $this->receiveErrorCount; }
    public function getConsumeOkCount() { return $this->consumeOkCount; }
    public function getConsumeErrorCount() { return $this->consumeErrorCount; }
    public function getAckCount() { return $this->ackCount; }
    public function getAckErrorCount() { return $this->ackErrorCount; }

    public function getAverageSendLatencyMs()
    {
        if (empty($this->sendLatencyMs)) {
            return 0;
        }
        return array_sum($this->sendLatencyMs) / count($this->sendLatencyMs);
    }

    public function getUptimeSeconds()
    {
        return time() - $this->startTime;
    }

    public function getStats()
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

    public function __construct()
    {
        $this->metrics = ClientMetrics::getInstance();
    }

    public function intercept($hookPoint, array $context = [])
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

    public function getMetrics()
    {
        return $this->metrics;
    }
}
