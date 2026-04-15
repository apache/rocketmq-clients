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

/**
 * Swoole coroutine support test class
 * 
 * Tests Swoole async operations
 */
class SwooleCoroutineTest extends TestCase
{
    /**
     * Test async send concept
     */
    public function testAsyncSend()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension not loaded');
        }
        
        // Basic Swoole availability test
        $this->assertTrue(function_exists('go'));
    }
    
    /**
     * Test concurrent batch concept
     */
    public function testConcurrentBatch()
    {
        if (!extension_loaded('swoole')) {
            $this->markTestSkipped('Swoole extension not loaded');
        }
        
        // Test Swoole Channel exists
        $this->assertTrue(class_exists('\\Swoole\\Coroutine\\Channel'));
    }
}
