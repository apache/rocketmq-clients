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

/**
 * Test framework utilities.
 */
class TestRunner
{
    public static $passed = 0;
    public static $failed = 0;
    public static $errors = 0;

    public static function assertTrue($value, $message = '')
    {
        if ($value === true) {
            self::$passed++;
            return;
        }
        self::$failed++;
        echo "  FAILED: Expected true, got " . var_export($value, true);
        if ($message) {
            echo " - {$message}";
        }
        echo "\n";
    }

    public static function assertFalse($value, $message = '')
    {
        if ($value === false) {
            self::$passed++;
            return;
        }
        self::$failed++;
        echo "  FAILED: Expected false, got " . var_export($value, true);
        if ($message) {
            echo " - {$message}";
        }
        echo "\n";
    }

    public static function assertEquals($expected, $actual, $message = '')
    {
        if ($expected === $actual) {
            self::$passed++;
            return;
        }
        self::$failed++;
        echo "  FAILED: Expected " . var_export($expected, true) . ", got " . var_export($actual, true);
        if ($message) {
            echo " - {$message}";
        }
        echo "\n";
    }

    public static function assertNotEquals($expected, $actual, $message = '')
    {
        if ($expected !== $actual) {
            self::$passed++;
            return;
        }
        self::$failed++;
        echo "  FAILED: Expected not equal to " . var_export($expected, true);
        if ($message) {
            echo " - {$message}";
        }
        echo "\n";
    }

    public static function assertNull($value, $message = '')
    {
        if ($value === null) {
            self::$passed++;
            return;
        }
        self::$failed++;
        echo "  FAILED: Expected null, got " . var_export($value, true);
        if ($message) {
            echo " - {$message}";
        }
        echo "\n";
    }

    public static function assertNotNull($value, $message = '')
    {
        if ($value !== null) {
            self::$passed++;
            return;
        }
        self::$failed++;
        echo "  FAILED: Expected non-null value";
        if ($message) {
            echo " - {$message}";
        }
        echo "\n";
    }

    public static function assertThrows($exceptionClass, $callable, $message = '')
    {
        try {
            $callable();
            self::$failed++;
            echo "  FAILED: Expected {$exceptionClass} to be thrown";
            if ($message) {
                echo " - {$message}";
            }
            echo "\n";
        } catch (\Throwable $e) {
            if ($e instanceof $exceptionClass || is_a($e, $exceptionClass)) {
                self::$passed++;
            } else {
                self::$failed++;
                echo "  FAILED: Expected {$exceptionClass}, got " . get_class($e);
                if ($message) {
                    echo " - {$message}";
                }
                echo "\n";
            }
        }
    }

    public static function assertTrueWithMessage($value, $message = '')
    {
        if ($value === true) {
            self::$passed++;
            return;
        }
        self::$failed++;
        echo "  FAILED: {$message}\n";
    }

    public static function assertEqualsWithMessage($expected, $actual, $message = '')
    {
        if ($expected === $actual) {
            self::$passed++;
            return;
        }
        self::$failed++;
        echo "  FAILED: {$message} (expected: {$expected}, actual: {$actual})\n";
    }
}
