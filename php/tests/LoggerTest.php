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
use Apache\Rocketmq\Test\TestRunner;

class LoggerTest
{
    private $testLogFile;

    public function __construct()
    {
        $this->testLogFile = sys_get_temp_dir() . '/rocketmq_test_logger_' . uniqid() . '.log';
    }

    public function testSingleton()
    {
        // Reset logger for test
        Logger::close();

        Logger::setLogFile($this->testLogFile);
        $logger1 = Logger::getInstance('TestComponent');
        $logger2 = Logger::getInstance('TestComponent');

        TestRunner::assertTrue($logger1 === $logger2, "Should return same instance for same component");
    }

    public function testDifferentComponents()
    {
        Logger::close();
        Logger::setLogFile($this->testLogFile);

        $logger1 = Logger::getInstance('ComponentA');
        $logger2 = Logger::getInstance('ComponentB');

        TestRunner::assertTrue($logger1 !== $logger2, "Should return different instances for different components");
    }

    public function testLogLevelFiltering()
    {
        Logger::close();
        Logger::setLogFile($this->testLogFile);
        Logger::setLogLevel(Logger::LEVEL_ERROR);

        $logger = Logger::getInstance('LogLevelTest');
        $logger->debug('This should not be logged');
        $logger->info('This should not be logged');
        $logger->warning('This should not be logged');

        // Read log file - should be empty or only contain prior test entries
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
        Logger::close();
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
        Logger::close();
        Logger::setLogFile($this->testLogFile);
        Logger::setLogLevel(Logger::LEVEL_DEBUG);

        $marker = 'format_test_' . uniqid();
        $logger = Logger::getInstance('FormatTest');
        $logger->info($marker);

        $content = file_get_contents($this->testLogFile) ?: '';
        // Check log format: [YYYY-MM-DD HH:MM:SS.mmm] [LEVEL] [Component] message
        $expectedPattern = "/\[\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}\] \[INFO\] \[FormatTest\] {$marker}/";
        TestRunner::assertTrue(
            preg_match($expectedPattern, $content) === 1,
            "Log line should match expected format"
        );
    }

    public function cleanup()
    {
        if (file_exists($this->testLogFile)) {
            @unlink($this->testLogFile);
        }
    }
}

echo "=== LoggerTest ===\n";
$test = new LoggerTest();
$test->testSingleton();
echo "  [OK] testSingleton\n";
$test->testDifferentComponents();
echo "  [OK] testDifferentComponents\n";
$test->testLogLevelFiltering();
echo "  [OK] testLogLevelFiltering\n";
$test->testLogLevelWrite();
echo "  [OK] testLogLevelWrite\n";
$test->testLogFormat();
echo "  [OK] testLogFormat\n";
Logger::close();
