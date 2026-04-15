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

/**
 * Message meter interceptor test class
 * 
 * Tests automatic metrics collection through interceptors
 */
class MessageMeterInterceptorTest extends TestCase
{
    /**
     * Test auto metrics collection
     */
    public function testAutoMetricsCollection()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        $meterManager->enable();
        
        // MessageMeterInterceptor requires: MetricsCollector, clientId, consumerGroup
        $interceptor = new MessageMeterInterceptor($metricsCollector, 'test-client', 'test-group');
        
        $this->assertInstanceOf(MessageMeterInterceptor::class, $interceptor);
    }
    
    /**
     * Test interceptor execution
     */
    public function testInterceptorExecution()
    {
        $metricsCollector = new MetricsCollector('test-client');
        $meterManager = new ClientMeterManager('test-client', $metricsCollector);
        $meterManager->enable();
        
        $interceptor = new MessageMeterInterceptor($metricsCollector, 'test-client', 'test-group');
        
        $message = (new \Apache\Rocketmq\Builder\MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->build();
        
        $context = new \Apache\Rocketmq\MessageInterceptorContext(
            \Apache\Rocketmq\MessageHookPoints::SEND_BEFORE,
            \Apache\Rocketmq\MessageHookPointsStatus::OK
        );
        
        // Should not throw exception
        $interceptor->doBefore($context, [$message]);
        
        $this->assertTrue(true);
    }
}
