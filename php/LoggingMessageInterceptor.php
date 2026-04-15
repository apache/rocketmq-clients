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
 * Logging Message Interceptor - Logs message send/consume operations
 * 
 * This interceptor demonstrates how to use the interceptor framework
 * for logging and monitoring purposes.
 */
class LoggingMessageInterceptor implements MessageInterceptor {
    /**
     * @var AttributeKey Key for storing start time
     */
    private static $START_TIME_KEY;
    
    /**
     * Initialize static properties
     */
    public function __construct() {
        if (self::$START_TIME_KEY === null) {
            self::$START_TIME_KEY = AttributeKey::create('message_start_time');
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function doBefore(MessageInterceptorContextInterface $context, array $messages): void {
        $hookPoint = $context->getMessageHookPoints();
        $hookPointName = MessageHookPoints::getName($hookPoint);
        
        // Record start time for performance monitoring
        $startTime = microtime(true);
        $context->putAttribute(self::$START_TIME_KEY, Attribute::create($startTime));
        
        foreach ($messages as $index => $message) {
            $messageId = method_exists($message, 'getMessageId') ? ($message->getMessageId() ?? 'N/A') : 'N/A';
            Logger::info("[{}] Processing message [index={}], topic={}, messageId={}", [
                $hookPointName,
                $index,
                $message->getTopic(),
                $messageId
            ]);
        }
    }
    
    /**
     * {@inheritdoc}
     */
    public function doAfter(MessageInterceptorContextInterface $context, array $messages): void {
        $hookPoint = $context->getMessageHookPoints();
        $status = $context->getStatus();
        $hookPointName = MessageHookPoints::getName($hookPoint);
        $statusName = MessageHookPointsStatus::getName($status);
        
        // Calculate processing time
        $startTimeAttr = $context->getAttribute(self::$START_TIME_KEY);
        $processingTimeMs = 0;
        
        if ($startTimeAttr !== null) {
            $startTime = $startTimeAttr->get();
            $endTime = microtime(true);
            $processingTimeMs = round(($endTime - $startTime) * 1000, 2);
        }
        
        foreach ($messages as $index => $message) {
            $messageId = method_exists($message, 'getMessageId') ? ($message->getMessageId() ?? 'N/A') : 'N/A';
            Logger::info("[{}] Message processed [index={}], status={}, processingTime={}ms, topic={}, messageId={}", [
                $hookPointName,
                $index,
                $statusName,
                $processingTimeMs,
                $message->getTopic(),
                $messageId
            ]);
        }
    }
}
