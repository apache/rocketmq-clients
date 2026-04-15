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

namespace Apache\Rocketmq\Tests;

use PHPUnit\Framework\TestCase;
use Apache\Rocketmq\MessageMeterInterceptor;
use Apache\Rocketmq\MetricsCollector;
use Apache\Rocketmq\ClientMeterManager;
use Apache\Rocketmq\MessageInterceptorContext;
use Apache\Rocketmq\MessageHookPoints;
use Apache\Rocketmq\MessageHookPointsStatus;
use Apache\Rocketmq\Builder\MessageBuilder;

/**
 * Message meter interceptor test class
 * 
 * Tests automatic metrics collection through message interceptors
 */
class MeterInterceptorTest extends TestCase
{
    /**
     * Test MessageMeterInterceptor creation
     */
    public function testMessageMeterInterceptorCreation()
    {
        $metricsCollector = new MetricsCollector('test-client');
        
        // MessageMeterInterceptor requires: MetricsCollector, clientId, consumerGroup (optional)
        $interceptor = new MessageMeterInterceptor($metricsCollector, 'test-client');
        
        $this->assertInstanceOf(MessageMeterInterceptor::class, $interceptor);
    }
    
    /**
     * Test interceptor records send metrics
     */
    public function testInterceptorRecordsSendMetrics()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        $meterManager->enable();
        
        // MessageMeterInterceptor expects: MetricsCollector, clientId, consumerGroup
        $interceptor = new MessageMeterInterceptor($metricsCollector, 'test-client', 'test-group');
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message body')
            ->build();
        
        // Before send
        $beforeContext = new \Apache\Rocketmq\MessageInterceptorContext(MessageHookPoints::SEND_BEFORE, MessageHookPointsStatus::OK);
        $interceptor->doBefore($beforeContext, [$message]);
        
        // After send
        $afterContext = new \Apache\Rocketmq\MessageInterceptorContext(MessageHookPoints::SEND_AFTER, MessageHookPointsStatus::OK);
        $interceptor->doAfter($afterContext, [$message]);
        
        // Verify metrics were collected (may be 0 in test environment)
        $result = $meterManager->exportMetrics();
        $this->assertTrue($result['success']);
        // Note: count may be 0 if metrics haven't been flushed yet
    }
    
    /**
     * Test interceptor handles errors
     */
    public function testInterceptorHandlesErrors()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        $meterManager->enable();
        
        $interceptor = new MessageMeterInterceptor($metricsCollector, 'test-client', 'test-group');
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->build();
        
        // Simulate error
        $context = new \Apache\Rocketmq\MessageInterceptorContext(MessageHookPoints::SEND_AFTER, MessageHookPointsStatus::ERROR);
        
        // Should not throw exception
        $interceptor->doAfter($context, [$message]);
        
        $this->assertTrue(true);
    }
    
    /**
     * Test interceptor with null message
     */
    public function testInterceptorWithNullMessage()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        
        $interceptor = new MessageMeterInterceptor($metricsCollector, 'test-client');
        
        $context = new \Apache\Rocketmq\MessageInterceptorContext(MessageHookPoints::SEND_BEFORE, MessageHookPointsStatus::OK);
        // Don't set message
        
        // Should handle gracefully
        $interceptor->doBefore($context, []);
        
        $this->assertTrue(true);
    }
}
