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

/**
 * Interceptor framework test class
 * 
 * Tests interceptor chain and custom interceptors
 */
class InterceptorFrameworkTest extends TestCase
{
    /**
     * Test interceptor chain management
     */
    public function testInterceptorChain()
    {
        $chain = new CompositedMessageInterceptor();
        
        $this->assertEquals(0, $chain->getInterceptorCount());
        
        $chain->addInterceptor(new LoggingMessageInterceptor());
        $this->assertEquals(1, $chain->getInterceptorCount());
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
        
        $message = (new \Apache\Rocketmq\Builder\MessageBuilder())
            ->setTopic('test-topic')
            ->setBody('Test message')
            ->build();
        
        $context = new \Apache\Rocketmq\MessageInterceptorContext(
            \Apache\Rocketmq\MessageHookPoints::SEND_BEFORE,
            \Apache\Rocketmq\MessageHookPointsStatus::OK
        );
        
        $customInterceptor->doBefore($context, [$message]);
        $this->assertTrue($customInterceptor->beforeCalled);
        
        $customInterceptor->doAfter($context, [$message]);
        $this->assertTrue($customInterceptor->afterCalled);
    }
}
