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
     * @return void
     */
    public static function setLogLevel($level)
    {
        self::$logLevel = $level;
    }

    /**
     * Set the log file path.
     *
     * @param string $path Absolute path to the log file
     * @return void
     */
    public static function setLogFile($path)
    {
        if (self::$handle !== null) {
            fclose(self::$handle);
            self::$handle = null;
        }
        self::$logFile = $path;
    }

    /**
     * Detect and correct timezone on first use.
     *
     * PHP CLI defaults to UTC when date.timezone is not set in php.ini.
     *
     * @return void
     */
    private static function initTimezone(): void
    {
        $tz = date_default_timezone_get();
        // 'UTC' means system timezone could not be determined
        if ($tz === 'UTC') {
            // Try common methods to detect system timezone
            $systemTz = null;

            // Method 1: /etc/localtime symlink (Linux/macOS)
            if ($systemTz === null && is_link('/etc/localtime')) {
                $link = readlink('/etc/localtime');
                if ($link !== false) {
                    // /usr/share/zoneinfo/Asia/Shanghai -> Asia/Shanghai
                    $parts = explode('/', $link);
                    if (count($parts) >= 4) {
                        $candidate = $parts[count($parts) - 2] . '/' . $parts[count($parts) - 1];
                        if (in_array($candidate, timezone_identifiers_list(), true)) {
                            $systemTz = $candidate;
                        }
                    }
                }
            }

            // Method 2: /etc/timezone file (Debian-based Linux)
            if ($systemTz === null) {
                $tzFile = @file_get_contents('/etc/timezone');
                if ($tzFile !== false) {
                    $candidate = trim($tzFile);
                    if (in_array($candidate, timezone_identifiers_list(), true)) {
                        $systemTz = $candidate;
                    }
                }
            }

            // Method 3: timedatectl (systemd-based Linux)
            if ($systemTz === null) {
                $output = @shell_exec('timedatectl show --property=Timezone --value 2>/dev/null');
                if ($output !== null) {
                    $candidate = trim($output);
                    if ($candidate !== '' && in_array($candidate, timezone_identifiers_list(), true)) {
                        $systemTz = $candidate;
                    }
                }
            }

            if ($systemTz !== null) {
                date_default_timezone_set($systemTz);
            }
        }
    }

    /**
     * Private constructor to enforce singleton-per-component pattern.
     *
     * @param string $component Component name for log line prefix
     */
    private function __construct($component)
    {
        $this->component = $component;
        self::initTimezone();
    }

    /**
     * Log a debug message.
     *
     * @param string $message The log message
     * @return void
     */
    public function debug($message)
    {
        $this->log(self::LEVEL_DEBUG, $message);
    }

    /**
     * Log an info message.
     *
     * @param string $message The log message
     * @return void
     */
    public function info($message)
    {
        $this->log(self::LEVEL_INFO, $message);
    }

    /**
     * Log a warning message.
     *
     * @param string $message The log message
     * @return void
     */
    public function warning($message)
    {
        $this->log(self::LEVEL_WARNING, $message);
    }

    /**
     * Log an error message.
     *
     * @param string $message The log message
     * @return void
     */
    public function error($message)
    {
        $this->log(self::LEVEL_ERROR, $message);
    }

    /**
     * Write a log entry to the log file with timestamp, level, and component prefix.
     *
     * @param int $level One of the LEVEL_* constants
     * @param string $message The log message
     * @return void
     */
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

    /**
     * Get or open the shared log file handle.
     *
     * @return resource|false The file handle, or false on failure
     */
    private function getHandle()
    {
        if (self::$handle !== null) {
            return self::$handle;
        }

        if (self::$logFile !== null) {
            $logFile = self::$logFile;
        } else {
            // Cross-platform home directory detection
            $home = getenv('HOME');
            if (empty($home)) {
                $home = getenv('USERPROFILE');
            }
            if (empty($home)) {
                $home = sys_get_temp_dir();
            }
            $logDir = $home . DIRECTORY_SEPARATOR . 'logs' . DIRECTORY_SEPARATOR . 'rocketmq';
            $logFile = $logDir . DIRECTORY_SEPARATOR . 'rocketmq_client_php.log';

            // Ensure log directory exists
            if (!is_dir($logDir)) {
                @mkdir($logDir, 0755, true);
            }
        }

        $handle = @fopen($logFile, 'a');
        if ($handle === false) {
            // Silently disable file logging if file cannot be opened
            self::$handle = false;
            return false;
        }

        self::$handle = $handle;
        return self::$handle;
    }

    /**
     * Close the log file handle. Called on shutdown.
     *
     * @return void
     */
    public static function close()
    {
        if (self::$handle !== null) {
            fclose(self::$handle);
            self::$handle = null;
        }
    }

    /**
     * Destructor. Does not close the shared handle.
     *
     * @return void
     */
    public function __destruct()
    {
        // Do not close here - handle is shared across all instances
    }
}
