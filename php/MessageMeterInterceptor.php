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

use Apache\Rocketmq\Message\MessageInterface;
use Apache\Rocketmq\Logger;

/**
 * Message Meter Interceptor - Automatically collects message metrics
 * 
 * This interceptor records:
 * - Send cost time (SEND_BEFORE/SEND_AFTER)
 * - Delivery latency (RECEIVE)
 * - Consume await time (CONSUME_BEFORE)
 * - Consume process time (CONSUME_AFTER)
 * 
 * Reference: Java MessageMeterInterceptor
 */
class MessageMeterInterceptor implements MessageInterceptor {
    /**
     * @var AttributeKey Key for send stopwatch
     */
    private static $SEND_STOPWATCH_KEY;
    
    /**
     * @var AttributeKey Key for consume stopwatch
     */
    private static $CONSUME_STOPWATCH_KEY;
    
    /**
     * @var MetricsCollector Metrics collector instance
     */
    private $metricsCollector;
    
    /**
     * @var string Client ID
     */
    private $clientId;
    
    /**
     * @var string|null Consumer group (for consumers)
     */
    private $consumerGroup;
    
    /**
     * Initialize static properties
     */
    public function __construct(MetricsCollector $metricsCollector, string $clientId, ?string $consumerGroup = null) {
        if (self::$SEND_STOPWATCH_KEY === null) {
            self::$SEND_STOPWATCH_KEY = AttributeKey::create('send_stopwatch');
        }
        if (self::$CONSUME_STOPWATCH_KEY === null) {
            self::$CONSUME_STOPWATCH_KEY = AttributeKey::create('consume_stopwatch');
        }
        
        $this->metricsCollector = $metricsCollector;
        $this->clientId = $clientId;
        $this->consumerGroup = $consumerGroup;
    }
    
    /**
     * {@inheritdoc}
     */
    public function doBefore(MessageInterceptorContextInterface $context, array $messages): void {
        $hookPoints = $context->getMessageHookPoints();
        
        switch ($hookPoints) {
            case MessageHookPoints::SEND_BEFORE:
                $this->doBeforeSendMessage($context);
                break;
                
            case MessageHookPoints::RECEIVE_BEFORE:
                $this->doBeforeReceiveMessage($context);
                break;
                
            case MessageHookPoints::CONSUME_BEFORE:
                $this->doBeforeConsumeMessage($context, $messages);
                break;
                
            case MessageHookPoints::ACK_BEFORE:
                $this->doBeforeAckMessage($context);
                break;
                
            default:
                // Do nothing for other hook points
                break;
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function doAfter(MessageInterceptorContextInterface $context, array $messages): void {
        $hookPoints = $context->getMessageHookPoints();
        
        switch ($hookPoints) {
            case MessageHookPoints::SEND_AFTER:
                $this->doAfterSendMessage($context, $messages);
                break;
                
            case MessageHookPoints::RECEIVE_AFTER:
                $this->doAfterReceiveMessage($context, $messages);
                break;
                
            case MessageHookPoints::CONSUME_AFTER:
                $this->doAfterConsumeMessage($context, $messages);
                break;
                
            case MessageHookPoints::ACK_AFTER:
                $this->doAfterAckMessage($context, $messages);
                break;
                
            default:
                // Do nothing for other hook points
                break;
        }
    }
    
    /**
     * Record time before sending message
     * 
     * @param MessageInterceptorContextInterface $context Interceptor context
     * @return void
     */
    private function doBeforeSendMessage(MessageInterceptorContextInterface $context): void {
        // Record the start time for measuring send duration
        $startTime = microtime(true);
        $context->putAttribute(
            self::$SEND_STOPWATCH_KEY,
            Attribute::create($startTime)
        );
    }
    
    /**
     * Record send cost time after sending message
     * 
     * @param MessageInterceptorContextInterface $context Interceptor context
     * @param \Apache\Rocketmq\Message\Message[] $messages Sent messages
     * @return void
     */
    private function doAfterSendMessage(MessageInterceptorContextInterface $context, array $messages): void {
        $stopwatchAttr = $context->getAttribute(self::$SEND_STOPWATCH_KEY);
        if ($stopwatchAttr === null) {
            // Should never reach here
            Logger::warn("Send stopwatch attribute not found, clientId={}", [$this->clientId]);
            return;
        }
        
        $startTime = $stopwatchAttr->get();
        $endTime = microtime(true);
        $durationMs = ($endTime - $startTime) * 1000; // Convert to milliseconds
        
        $status = $context->getStatus() === MessageHookPointsStatus::OK
            ? InvocationStatus::SUCCESS
            : InvocationStatus::FAILURE;
        
        foreach ($messages as $message) {
            $labels = [
                MetricLabels::TOPIC => $message->getTopic(),
                MetricLabels::CLIENT_ID => $this->clientId,
                MetricLabels::INVOCATION_STATUS => $status,
            ];
            
            $this->metricsCollector->observeHistogram(
                HistogramEnum::SEND_COST_TIME,
                $labels,
                $durationMs
            );
            
            Logger::debug("Recorded send metric: topic={}, status={}, duration={}ms", [
                $message->getTopic(),
                $status,
                round($durationMs, 2)
            ]);
        }
    }
    
    /**
     * Record consume await time before consuming message
     * 
     * @param MessageInterceptorContextInterface $context Interceptor context
     * @param \Apache\Rocketmq\Message\Message[] $messages Messages to consume
     * @return void
     */
    private function doBeforeConsumeMessage(MessageInterceptorContextInterface $context, array $messages): void {
        if (empty($messages)) {
            return;
        }
        
        if ($this->consumerGroup === null) {
            Logger::error("[Bug] consumerGroup is not recognized, clientId={}", [$this->clientId]);
            return;
        }
        
        $message = $messages[0];
        
        // Try to get decode timestamp from message properties
        $decodeTimestamp = $this->getDecodeTimestamp($message);
        if ($decodeTimestamp !== null) {
            $currentTimeMs = microtime(true) * 1000;
            $awaitTimeMs = $currentTimeMs - $decodeTimestamp;
            
            if ($awaitTimeMs >= 0) {
                $labels = [
                    MetricLabels::TOPIC => $message->getTopic(),
                    MetricLabels::CONSUMER_GROUP => $this->consumerGroup,
                    MetricLabels::CLIENT_ID => $this->clientId,
                ];
                
                $this->metricsCollector->observeHistogram(
                    HistogramEnum::AWAIT_TIME,
                    $labels,
                    $awaitTimeMs
                );
                
                Logger::debug("Recorded await time metric: topic={}, awaitTime={}ms", [
                    $message->getTopic(),
                    round($awaitTimeMs, 2)
                ]);
            } else {
                Logger::debug("Await time is negative, awaitTime={}ms", [$awaitTimeMs]);
            }
        }
        
        // Record the start time for measuring consume duration
        $startTime = microtime(true);
        $context->putAttribute(
            self::$CONSUME_STOPWATCH_KEY,
            Attribute::create($startTime)
        );
    }
    
    /**
     * Record consume process time after consuming message
     * 
     * @param MessageInterceptorContextInterface $context Interceptor context
     * @param \Apache\Rocketmq\Message\Message[] $messages Consumed messages
     * @return void
     */
    private function doAfterConsumeMessage(MessageInterceptorContextInterface $context, array $messages): void {
        if ($this->consumerGroup === null) {
            Logger::error("[Bug] consumerGroup is not recognized, clientId={}", [$this->clientId]);
            return;
        }
        
        $stopwatchAttr = $context->getAttribute(self::$CONSUME_STOPWATCH_KEY);
        if ($stopwatchAttr === null) {
            // Should never reach here
            Logger::warn("Consume stopwatch attribute not found, clientId={}", [$this->clientId]);
            return;
        }
        
        $startTime = $stopwatchAttr->get();
        $endTime = microtime(true);
        $durationMs = ($endTime - $startTime) * 1000; // Convert to milliseconds
        
        $status = $context->getStatus() === MessageHookPointsStatus::OK
            ? InvocationStatus::SUCCESS
            : InvocationStatus::FAILURE;
        
        foreach ($messages as $message) {
            $labels = [
                MetricLabels::TOPIC => $message->getTopic(),
                MetricLabels::CONSUMER_GROUP => $this->consumerGroup,
                MetricLabels::CLIENT_ID => $this->clientId,
                MetricLabels::INVOCATION_STATUS => $status,
            ];
            
            $this->metricsCollector->observeHistogram(
                HistogramEnum::PROCESS_TIME,
                $labels,
                $durationMs
            );
            
            Logger::debug("Recorded process time metric: topic={}, status={}, duration={}ms", [
                $message->getTopic(),
                $status,
                round($durationMs, 2)
            ]);
        }
    }
    
    /**
     * Get decode timestamp from message
     * 
     * @param \Apache\Rocketmq\Message\Message $message Message
     * @return float|null Decode timestamp in milliseconds, or null if not available
     */
    private function getDecodeTimestamp(\Apache\Rocketmq\Message\Message $message): ?float {
        // Try to get from system properties
        try {
            $properties = $message->getProperties();
            if (isset($properties['DECODE_TIMESTAMP'])) {
                return floatval($properties['DECODE_TIMESTAMP']);
            }
            
            // Alternative: check for BORN_TIMESTAMP
            if (isset($properties['BORN_TIMESTAMP'])) {
                return floatval($properties['BORN_TIMESTAMP']);
            }
        } catch (\Exception $e) {
            Logger::debug("Failed to get decode timestamp: {}", [$e->getMessage()]);
        }
        
        return null;
    }
    
    /**
     * Record time before receiving message
     * 
     * @param MessageInterceptorContextInterface $context Interceptor context
     * @return void
     */
    private function doBeforeReceiveMessage(MessageInterceptorContextInterface $context): void {
        // Record the start time for measuring receive duration
        $startTime = microtime(true);
        $context->putAttribute(
            self::$SEND_STOPWATCH_KEY,
            Attribute::create($startTime)
        );
    }
    
    /**
     * Record receive metrics after receiving message
     * 
     * @param MessageInterceptorContextInterface $context Interceptor context
     * @param MessageInterface[] $messages Received messages
     * @return void
     */
    private function doAfterReceiveMessage(MessageInterceptorContextInterface $context, array $messages): void {
        if (empty($messages)) {
            return;
        }
        
        $stopwatchAttr = $context->getAttribute(self::$SEND_STOPWATCH_KEY);
        if ($stopwatchAttr === null) {
            Logger::warn("Receive stopwatch attribute not found, clientId={}", [$this->clientId]);
            return;
        }
        
        $startTime = $stopwatchAttr->get();
        $endTime = microtime(true);
        $durationMs = ($endTime - $startTime) * 1000;
        
        $status = $context->getStatus() === MessageHookPointsStatus::OK
            ? InvocationStatus::SUCCESS
            : InvocationStatus::FAILURE;
        
        foreach ($messages as $message) {
            $labels = [
                MetricLabels::TOPIC => $message->getTopic(),
                MetricLabels::CLIENT_ID => $this->clientId,
                MetricLabels::INVOCATION_STATUS => $status,
            ];
            
            $this->metricsCollector->observeHistogram(
                HistogramEnum::RECEIVE_COST_TIME,
                $labels,
                $durationMs
            );
        }
    }
    
    /**
     * Record time before acknowledging message
     * 
     * @param MessageInterceptorContextInterface $context Interceptor context
     * @return void
     */
    private function doBeforeAckMessage(MessageInterceptorContextInterface $context): void {
        $startTime = microtime(true);
        $context->putAttribute(
            self::$SEND_STOPWATCH_KEY,
            Attribute::create($startTime)
        );
    }
    
    /**
     * Record ack metrics after acknowledging message
     * 
     * @param MessageInterceptorContextInterface $context Interceptor context
     * @param MessageInterface[] $messages Acknowledged messages
     * @return void
     */
    private function doAfterAckMessage(MessageInterceptorContextInterface $context, array $messages): void {
        if (empty($messages)) {
            return;
        }
        
        $stopwatchAttr = $context->getAttribute(self::$SEND_STOPWATCH_KEY);
        if ($stopwatchAttr === null) {
            return;
        }
        
        $startTime = $stopwatchAttr->get();
        $endTime = microtime(true);
        $durationMs = ($endTime - $startTime) * 1000;
        
        $status = $context->getStatus() === MessageHookPointsStatus::OK
            ? InvocationStatus::SUCCESS
            : InvocationStatus::FAILURE;
        
        foreach ($messages as $message) {
            $labels = [
                MetricLabels::TOPIC => $message->getTopic(),
                MetricLabels::CLIENT_ID => $this->clientId,
                MetricLabels::INVOCATION_STATUS => $status,
            ];
            
            $this->metricsCollector->observeHistogram(
                HistogramEnum::ACK_COST_TIME,
                $labels,
                $durationMs
            );
        }
    }
}
