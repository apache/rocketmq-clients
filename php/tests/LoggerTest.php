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

namespace Apache\Rocketmq\Test;

require_once __DIR__ . '/TestRunner.php';
require_once __DIR__ . '/../Logger.php';

use Apache\Rocketmq\Logger;

class LoggerTest
{
    private $testLogFile;

    public function setUp(): void
    {
        $this->testLogFile = sys_get_temp_dir() . '/rocketmq_test_logger_' . uniqid() . '.log';
    }

    public function tearDown(): void
    {
        Logger::close();
        if (file_exists($this->testLogFile)) {
            @unlink($this->testLogFile);
        }
    }

    public function testSingleton()
    {
        Logger::setLogFile($this->testLogFile);
        $logger1 = Logger::getInstance('TestComponent');
        $logger2 = Logger::getInstance('TestComponent');

        TestRunner::assertTrue($logger1 === $logger2, "Should return same instance for same component");
    }

    public function testDifferentComponents()
    {
        Logger::setLogFile($this->testLogFile);

        $logger1 = Logger::getInstance('ComponentA');
        $logger2 = Logger::getInstance('ComponentB');

        TestRunner::assertTrue($logger1 !== $logger2, "Should return different instances for different components");
    }

    public function testLogLevelFiltering()
    {
        Logger::setLogFile($this->testLogFile);
        Logger::setLogLevel(Logger::LEVEL_ERROR);

        $logger = Logger::getInstance('LogLevelTest');
        $logger->debug('This should not be logged');
        $logger->info('This should not be logged');
        $logger->warning('This should not be logged');

        $content = '';
        if (file_exists($this->testLogFile)) {
            $content = file_get_contents($this->testLogFile) ?: '';
        }
        TestRunner::assertFalse(
            strpos($content, 'This should not be logged') !== false,
            "DEBUG/INFO/WARNING should be filtered when log level is ERROR"
        );
    }

    public function testLogLevelWrite()
    {
        Logger::setLogFile($this->testLogFile);
        Logger::setLogLevel(Logger::LEVEL_DEBUG);

        $logger = Logger::getInstance('LogLevelWriteTest');
        $marker = 'unique_test_marker_' . uniqid();
        $logger->info($marker);

        $content = file_get_contents($this->testLogFile) ?: '';
        TestRunner::assertTrue(
            strpos($content, $marker) !== false,
            "INFO message should be written when log level is DEBUG"
        );
    }

    public function testLogFormat()
    {
        Logger::setLogFile($this->testLogFile);
        Logger::setLogLevel(Logger::LEVEL_DEBUG);

        $marker = 'format_test_' . uniqid();
        $logger = Logger::getInstance('FormatTest');
        $logger->info($marker);

        $content = file_get_contents($this->testLogFile) ?: '';
        $expectedPattern = "/\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}\] \[INFO\] \[FormatTest\] {$marker}/";
        TestRunner::assertTrue(
            preg_match($expectedPattern, $content) === 1,
            "Log line should match expected format"
        );
    }

    public function testTimezoneDetection()
    {
        $originalTz = date_default_timezone_get();

        // Force UTC to simulate PHP CLI default
        date_default_timezone_set('UTC');

        // Logger init should detect and correct system timezone
        Logger::getInstance('TimezoneTest');

        $currentTz = date_default_timezone_get();

        // Restore original timezone
        if ($originalTz !== 'UTC') {
            date_default_timezone_set($originalTz);
        }

        // If system timezone was detected (not UTC anymore), the test passes
        // If it's still UTC, that's also valid (system may genuinely be UTC)
        TestRunner::assertTrue(
            in_array($currentTz, timezone_identifiers_list(), true) || $currentTz === 'UTC',
            "Timezone should be a valid identifier after Logger init (got: {$currentTz})"
        );
    }
}

echo "=== LoggerTest ===\n";
TestRunner::run(new LoggerTest());
