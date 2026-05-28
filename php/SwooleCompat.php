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

/**
 * SwooleCompat - Swoole coroutine availability detection and helpers.
 */
class SwooleCompat
{
    /**
     * Check if Swoole or OpenSwoole extension is loaded.
     */
    public static function isAvailable(): bool
    {
        return extension_loaded('swoole') || extension_loaded('openswoole');
    }

    /**
     * Check if currently running inside a Swoole coroutine.
     */
    public static function inCoroutine(): bool
    {
        if (!self::isAvailable()) {
            return false;
        }
        return \Swoole\Coroutine::getCid() > 0;
    }

    /**
     * Run a callback in a coroutine if not already in one.
     * If Swoole is not available, executes synchronously.
     *
     * @param callable $fn
     * @return mixed
     */
    public static function runInCoroutine(callable $fn, float $timeout = 30.0)
    {
        if (!self::isAvailable()) {
            return $fn();
        }
        if (self::inCoroutine()) {
            return $fn();
        }
        $result = null;
        $exception = null;
        $channel = new \Swoole\Coroutine\Channel(1);
        \Swoole\Coroutine::create(function () use ($fn, $channel, &$result, &$exception) {
            try {
                $result = $fn();
            } catch (\Throwable $e) {
                $exception = $e;
            }
            $channel->push(true);
        });
        $popped = $channel->pop($timeout);
        if ($popped === false) {
            throw new \RuntimeException("Swoole execution timed out after {$timeout}");
        }
        if ($exception !== null) {
            throw $exception;
        }
        return $result;
    }

    /**
     * Sleep for specified microseconds, using coroutine-friendly sleep if in Swoole context.
     * 
     * In Swoole coroutine: Uses \Swoole\Coroutine::sleep() (non-blocking)
     * In traditional PHP: Uses usleep() (blocking)
     *
     * @param int $microseconds Sleep duration in microseconds
     * @return void
     */
    public static function sleep(int $microseconds): void
    {
        if (self::isAvailable() && self::inCoroutine()) {
            // Convert microseconds to seconds for Swoole coroutine sleep
            $seconds = $microseconds / 1000000.0;
            \Swoole\Coroutine::sleep($seconds);
        } else {
            // Traditional blocking sleep
            usleep($microseconds);
        }
    }
}
