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
use Apache\Rocketmq\CompositedMessageInterceptor;
use Apache\Rocketmq\LoggingMessageInterceptor;
use \Apache\Rocketmq\MessageInterceptorContext;
use Apache\Rocketmq\MessageHookPoints;
use Apache\Rocketmq\MessageHookPointsStatus;
use Apache\Rocketmq\Builder\MessageBuilder;

/**
 * Interceptor framework test class
 * 
 * Tests message interceptor functionality including:
 * - CompositedMessageInterceptor chain management
 * - LoggingMessageInterceptor
 * - Custom interceptors
 */
class InterceptorTest extends TestCase
{
    /**
     * Test CompositedMessageInterceptor basic functionality
     */
    public function testCompositedInterceptorBasic()
    {
        $interceptorChain = new CompositedMessageInterceptor();
        
        $this->assertEquals(0, $interceptorChain->getInterceptorCount());
    }
    
    /**
     * Test adding interceptors
     */
    public function testAddInterceptor()
    {
        $interceptorChain = new CompositedMessageInterceptor();
        
        $loggingInterceptor = new LoggingMessageInterceptor();
        $interceptorChain->addInterceptor($loggingInterceptor);
        
        $this->assertEquals(1, $interceptorChain->getInterceptorCount());
    }
    
    /**
     * Test adding multiple interceptors
     */
    public function testAddMultipleInterceptors()
    {
        $interceptorChain = new CompositedMessageInterceptor();
        
        $interceptorChain->addInterceptor(new LoggingMessageInterceptor());
        $interceptorChain->addInterceptor(new LoggingMessageInterceptor());
        $interceptorChain->addInterceptor(new LoggingMessageInterceptor());
        
        $this->assertEquals(3, $interceptorChain->getInterceptorCount());
    }
    
    /**
     * Test custom interceptor implementation
     */
    public function testCustomInterceptor()
    {
        $customInterceptor = new class implements \Apache\Rocketmq\MessageInterceptor {
            public $beforeCalled = false;
            public $afterCalled = false;
            
            public function doBefore(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {
                $this->beforeCalled = true;
            }
            
            public function doAfter(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {
                $this->afterCalled = true;
            }
        };
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->build();
        
        $context = new MessageInterceptorContext(MessageHookPoints::SEND_BEFORE, MessageHookPointsStatus::OK);
        
        // Call interceptor
        $customInterceptor->doBefore($context, [$message]);
        $this->assertTrue($customInterceptor->beforeCalled);
        
        $customInterceptor->doAfter($context, [$message]);
        $this->assertTrue($customInterceptor->afterCalled);
    }
    
    /**
     * Test interceptor chain execution
     */
    public function testInterceptorChainExecution()
    {
        $executionOrder = [];
        
        $interceptor1 = new class($executionOrder) implements \Apache\Rocketmq\MessageInterceptor {
            private $order;
            public function __construct(&$order) { $this->order = &$order; }
            public function doBefore(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {
                $this->order[] = 'interceptor1-before';
            }
            public function doAfter(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {
                $this->order[] = 'interceptor1-after';
            }
        };
        
        $interceptor2 = new class($executionOrder) implements \Apache\Rocketmq\MessageInterceptor {
            private $order;
            public function __construct(&$order) { $this->order = &$order; }
            public function doBefore(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {
                $this->order[] = 'interceptor2-before';
            }
            public function doAfter(\Apache\Rocketmq\MessageInterceptorContextInterface $context, array $messages): void {
                $this->order[] = 'interceptor2-after';
            }
        };
        
        $chain = new CompositedMessageInterceptor();
        $chain->addInterceptor($interceptor1);
        $chain->addInterceptor($interceptor2);
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->build();
        
        $context = new MessageInterceptorContext(MessageHookPoints::SEND_BEFORE, MessageHookPointsStatus::OK);
        
        // Execute chain
        $chain->doBefore($context, [$message]);
        
        $this->assertEquals([
            'interceptor1-before',
            'interceptor2-before'
        ], $executionOrder);
    }
    
    /**
     * Test LoggingMessageInterceptor
     */
    public function testLoggingInterceptor()
    {
        $interceptor = new LoggingMessageInterceptor();
        
        $message = (new MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->build();
        
        $context = new MessageInterceptorContext(MessageHookPoints::SEND_BEFORE, MessageHookPointsStatus::OK);
        
        // Should not throw exception
        $interceptor->doBefore($context, [$message]);
        $interceptor->doAfter($context, [$message]);
        
        $this->assertTrue(true); // If we get here, it worked
    }
}
