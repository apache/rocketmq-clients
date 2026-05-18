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
 * Logger - Unified logging for the PHP RocketMQ client.
 *
 * Writes to rocketmq_client_php.log with format:
 * [YYYY-MM-DD HH:MM:SS.mmm] [LEVEL] [Component] message
 *
 * Usage:
 *   $logger = Logger::getInstance('Producer');
 *   $logger->info('Producer started');
 */
class Logger
{
    const LEVEL_DEBUG = 0;
    const LEVEL_INFO = 1;
    const LEVEL_WARNING = 2;
    const LEVEL_ERROR = 3;

    private static $instances = [];
    private static $logLevel = self::LEVEL_INFO;
    private static $logFile = null;
    private static $handle = null;

    private $component;

    /**
     * Get or create a Logger instance for the given component.
     *
     * @param string $component Component name (e.g., 'Producer', 'SimpleConsumer')
     * @return Logger
     */
    public static function getInstance($component)
    {
        if (!isset(self::$instances[$component])) {
            self::$instances[$component] = new self($component);
        }
        return self::$instances[$component];
    }

    /**
     * Set the minimum log level.
     *
     * @param int $level One of the LEVEL_* constants
     */
    public static function setLogLevel($level)
    {
        self::$logLevel = $level;
    }

    /**
     * Set the log file path.
     *
     * @param string $path Absolute path to the log file
     */
    public static function setLogFile($path)
    {
        if (self::$handle !== null) {
            fclose(self::$handle);
            self::$handle = null;
        }
        self::$logFile = $path;
    }

    private function __construct($component)
    {
        $this->component = $component;
    }

    public function debug($message)
    {
        $this->log(self::LEVEL_DEBUG, $message);
    }

    public function info($message)
    {
        $this->log(self::LEVEL_INFO, $message);
    }

    public function warning($message)
    {
        $this->log(self::LEVEL_WARNING, $message);
    }

    public function error($message)
    {
        $this->log(self::LEVEL_ERROR, $message);
    }

    private function log($level, $message)
    {
        if ($level < self::$logLevel) {
            return;
        }

        $levelNames = [
            self::LEVEL_DEBUG => 'DEBUG',
            self::LEVEL_INFO => 'INFO',
            self::LEVEL_WARNING => 'WARNING',
            self::LEVEL_ERROR => 'ERROR',
        ];

        $ts = microtime(true);
        $sec = floor($ts);
        $ms = (int)(($ts - $sec) * 1000);
        $timeStr = date('Y-m-d H:i:s', $sec) . '.' . str_pad((string)$ms, 3, '0', STR_PAD_LEFT);

        $line = "[$timeStr] [{$levelNames[$level]}] [{$this->component}] $message\n";

        $handle = $this->getHandle();
        if ($handle !== false) {
            flock($handle, LOCK_EX);
            fwrite($handle, $line);
            fflush($handle);
            flock($handle, LOCK_UN);
        }
    }

    private function getHandle()
    {
        if (self::$handle !== null) {
            return self::$handle;
        }

        $logFile = self::$logFile ?? (__DIR__ . '/rocketmq_client_php.log');
        $handle = @fopen($logFile, 'a');
        if ($handle === false) {
            // Fallback to error_log if file cannot be opened
            error_log("[Logger] Failed to open log file: $logFile");
            return false;
        }

        self::$handle = $handle;
        return self::$handle;
    }

    /**
     * Close the log file handle. Called on shutdown.
     */
    public static function close()
    {
        if (self::$handle !== null) {
            fclose(self::$handle);
            self::$handle = null;
        }
    }

    public function __destruct()
    {
        // Do not close here - handle is shared across all instances
    }
}
