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
    public static $currentTest = '';

    public static function assertTrue($value, string $message = ''): void
    {
        if ($value === true) {
            self::$passed++;
            return;
        }
        self::fail("Expected true, got " . var_export($value, true), $message);
    }

    public static function assertFalse($value, string $message = ''): void
    {
        if ($value === false) {
            self::$passed++;
            return;
        }
        self::fail("Expected false, got " . var_export($value, true), $message);
    }

    public static function assertEquals($expected, $actual, string $message = ''): void
    {
        if ($expected === $actual) {
            self::$passed++;
            return;
        }
        self::fail("{$message} (expected: " . var_export($expected, true) . ", actual: " . var_export($actual, true) . ")");
    }

    public static function assertNotEquals($expected, $actual, string $message = ''): void
    {
        if ($expected !== $actual) {
            self::$passed++;
            return;
        }
        self::fail("Expected not equal to " . var_export($expected, true), $message);
    }

    public static function assertNull($value, string $message = ''): void
    {
        if ($value === null) {
            self::$passed++;
            return;
        }
        self::fail("Expected null, got " . var_export($value, true), $message);
    }

    public static function assertNotNull($value, string $message = ''): void
    {
        if ($value !== null) {
            self::$passed++;
            return;
        }
        self::fail("Expected non-null value", $message);
    }

    public static function assertThrows(string $exceptionClass, callable $callable, string $message = ''): void
    {
        try {
            $callable();
            self::fail("Expected {$exceptionClass} to be thrown", $message);
        } catch (\Throwable $e) {
            if ($e instanceof $exceptionClass) {
                self::$passed++;
            } else {
                self::fail("Expected {$exceptionClass}, got " . get_class($e), $message);
            }
        }
    }

    public static function fail(string $detail, string $message = ''): void
    {
        self::$failed++;
        $output = "  FAILED";
        if (self::$currentTest !== '') {
            $output .= " [" . self::$currentTest . "]";
        }
        $output .= ": {$detail}";
        if ($message) {
            $output .= " - {$message}";
        }
        echo "{$output}\n";
    }

    /**
     * Run all test* methods on a test class instance.
     */
    public static function run(object $testClass): void
    {
        $ref = new \ReflectionClass($testClass);
        $methods = $ref->getMethods(\ReflectionMethod::IS_PUBLIC);

        foreach ($methods as $method) {
            if (str_starts_with($method->getName(), 'test') && $method->isStatic() === false) {
                self::$currentTest = $method->getName();
                if (method_exists($testClass, 'setUp')) {
                    $testClass->setUp();
                }
                try {
                    $method->invoke($testClass);
                    echo "  [OK] {$method->getName()}\n";
                } catch (\Throwable $e) {
                    self::$errors++;
                    echo "  ERROR [{$method->getName()}]: " . $e->getMessage() . "\n";
                }
                if (method_exists($testClass, 'tearDown')) {
                    $testClass->tearDown();
                }
            }
        }
        self::$currentTest = '';
    }

    /**
     * Run all test files matching the pattern and report summary.
     */
    public static function runAll(array $files): void
    {
        foreach ($files as $file) {
            require_once $file;
        }
    }

    public static function report(): void
    {
        echo "\n";
        echo "Passed: " . self::$passed . "\n";
        echo "Failed: " . self::$failed . "\n";
        if (self::$errors > 0) {
            echo "Errors: " . self::$errors . "\n";
        }
        $total = self::$passed + self::$failed + self::$errors;
        echo "Total:  {$total}\n";
        if (self::$failed > 0 || self::$errors > 0) {
            exit(1);
        }
    }
}
