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
 * Logger class for RocketMQ PHP client
 * 
 * Provides centralized logging functionality that writes to rocketmq_client_php.log
 */
class Logger {
    /**
     * @var string Log file path
     */
    private static $logFile;
    
    /**
     * @var bool Whether logging is enabled
     */
    private static $enabled = true;
    
    /**
     * @var int Minimum log level (DEBUG=0, INFO=1, WARN=2, ERROR=3)
     */
    private static $minLevel = 0;
    
    /**
     * @var resource|null File handle
     */
    private static $fileHandle = null;
    
    /**
     * @var int Maximum log file size in bytes (default: 10MB)
     */
    private static $maxFileSize = 10485760; // 10MB
    
    /**
     * @var int Maximum number of backup files (default: 5)
     */
    private static $maxBackupFiles = 5;
    
    /**
     * @var bool Whether to enable performance timing
     */
    private static $enableTiming = false;
    
    /**
     * @var array Performance timers storage
     */
    private static $timers = [];
    
    /**
     * Log levels
     */
    const DEBUG = 0;
    const INFO = 1;
    const WARN = 2;
    const ERROR = 3;
    
    /**
     * Initialize logger
     * 
     * @param string|null $logFile Log file path (default: ~/logs/rocketmq/rocketmq_client_php.log)
     * @param bool $enabled Whether logging is enabled
     * @param int $minLevel Minimum log level
     * @return void
     */
    public static function init($logFile = null, $enabled = true, $minLevel = self::DEBUG) {
        // Set default log file to user's home directory
        if ($logFile === null) {
            $homeDir = getenv('HOME');
            if ($homeDir === false) {
                // Fallback for Windows or if HOME is not set
                $homeDir = sys_get_temp_dir();
            }
            $logDir = $homeDir . DIRECTORY_SEPARATOR . 'logs' . DIRECTORY_SEPARATOR . 'rocketmq';
            $logFile = $logDir . DIRECTORY_SEPARATOR . 'rocketmq_client_php.log';
        }
        
        self::$logFile = $logFile;
        self::$enabled = $enabled;
        self::$minLevel = $minLevel;
        
        // Open file handle if logging is enabled
        if (self::$enabled) {
            self::openLogFile();
        }
        
        // Log initialization
        self::info("PHP RocketMQ client logger initialized, logFile={}, minLevel={}", [
            $logFile,
            self::getLevelString($minLevel)
        ]);
    }
    
    /**
     * Open log file
     * 
     * @return void
     */
    private static function openLogFile() {
        // Get absolute path
        $logPath = self::$logFile;
        if (!preg_match('/^\//', $logPath)) {
            // Relative path, make it absolute based on current directory
            $logPath = getcwd() . DIRECTORY_SEPARATOR . $logPath;
        }
        
        // Ensure directory exists
        $dir = dirname($logPath);
        if (!is_dir($dir)) {
            mkdir($dir, 0755, true);
        }
        
        // Check if log rotation is needed
        self::rotateLogIfNeeded($logPath);
        
        // Open file in append mode
        self::$fileHandle = fopen($logPath, 'a');
        if (self::$fileHandle === false) {
            // Fallback to error_log if file cannot be opened
            error_log("Failed to open log file: {$logPath}");
            self::$fileHandle = null;
        }
    }
    
    /**
     * Rotate log file if size exceeds limit
     * 
     * @param string $logPath Log file path
     * @return void
     */
    private static function rotateLogIfNeeded($logPath) {
        if (!file_exists($logPath)) {
            return;
        }
        
        $fileSize = filesize($logPath);
        if ($fileSize < self::$maxFileSize) {
            return;
        }
        
        // Close current file handle if open
        if (self::$fileHandle !== null) {
            fclose(self::$fileHandle);
            self::$fileHandle = null;
        }
        
        // Rotate backup files
        for ($i = self::$maxBackupFiles - 1; $i >= 1; $i--) {
            $oldFile = $logPath . '.' . $i;
            $newFile = $logPath . '.' . ($i + 1);
            
            if (file_exists($oldFile)) {
                if ($i === self::$maxBackupFiles - 1) {
                    // Delete oldest backup
                    unlink($oldFile);
                } else {
                    // Rename to next number
                    rename($oldFile, $newFile);
                }
            }
        }
        
        // Rename current log to .1
        if (file_exists($logPath)) {
            rename($logPath, $logPath . '.1');
        }
    }
    
    /**
     * Close log file
     * 
     * @return void
     */
    public static function close() {
        if (self::$fileHandle !== null) {
            fclose(self::$fileHandle);
            self::$fileHandle = null;
        }
    }
    
    /**
     * Write log message
     * 
     * @param int $level Log level
     * @param string $message Log message with {} placeholders (SLF4J style)
     * @param array $context Context data for placeholders or additional info
     * @return void
     */
    public static function log($level, $message, $context = []) {
        if (!self::$enabled || $level < self::$minLevel) {
            return;
        }
        
        // Format timestamp with milliseconds (similar to Java)
        $timestamp = date('Y-m-d H:i:s') . '.' . sprintf('%03d', (int)(microtime(true) * 1000) % 1000);
        
        // Format level
        $levelStr = self::getLevelString($level);
        
        // Extract caller information (class and method)
        $callerInfo = self::getCallerInfo();
        
        // Replace placeholders in message (SLF4J style: {} )
        $formattedMessage = self::formatMessage($message, $context);
        
        // Format context (exclude placeholder values)
        $contextStr = self::formatContext($context);
        
        // Format log line: [timestamp] [level] [caller] message context
        $logLine = "[{$timestamp}] [{$levelStr}] [{$callerInfo}] {$formattedMessage}{$contextStr}" . PHP_EOL;
        
        // Write to file
        if (self::$fileHandle !== null) {
            fwrite(self::$fileHandle, $logLine);
            fflush(self::$fileHandle); // Flush immediately
        } else {
            // Fallback to error_log
            error_log($logLine);
        }
    }
    
    /**
     * Debug level log
     * 
     * @param string $message Log message
     * @param array $context Context data
     * @return void
     */
    public static function debug($message, $context = []) {
        self::log(self::DEBUG, $message, $context);
    }
    
    /**
     * Info level log
     * 
     * @param string $message Log message
     * @param array $context Context data
     * @return void
     */
    public static function info($message, $context = []) {
        self::log(self::INFO, $message, $context);
    }
    
    /**
     * Warning level log
     * 
     * @param string $message Log message
     * @param array $context Context data
     * @return void
     */
    public static function warn($message, $context = []) {
        self::log(self::WARN, $message, $context);
    }
    
    /**
     * Error level log
     * 
     * @param string $message Log message
     * @param array $context Context data
     * @param \Throwable|null $exception Optional exception to log stack trace
     * @return void
     */
    public static function error($message, $context = [], $exception = null) {
        // If exception is provided and not in context, add it
        if ($exception !== null && !isset($context['error'])) {
            $context['error'] = $exception->getMessage();
        }
        
        self::log(self::ERROR, $message, $context);
        
        // Log stack trace if exception is provided and level allows
        if ($exception !== null && self::$enabled && self::ERROR >= self::$minLevel) {
            $stackTrace = $exception->getTraceAsString();
            $traceLines = explode("\n", $stackTrace);
            
            // Limit stack trace to first 10 lines to avoid excessive logging
            $limitedTrace = array_slice($traceLines, 0, 10);
            
            foreach ($limitedTrace as $line) {
                self::log(self::ERROR, "  at {$line}", []);
            }
            
            if (count($traceLines) > 10) {
                self::log(self::ERROR, "  ... and " . (count($traceLines) - 10) . " more", []);
            }
        }
    }
    
    /**
     * Get level string
     * 
     * @param int $level Log level
     * @return string Level string
     */
    private static function getLevelString($level) {
        switch ($level) {
            case self::DEBUG:
                return 'DEBUG';
            case self::INFO:
                return 'INFO';
            case self::WARN:
                return 'WARN';
            case self::ERROR:
                return 'ERROR';
            default:
                return 'UNKNOWN';
        }
    }
    
    /**
     * Get caller information (class and method)
     * 
     * @return string Caller information in format "Class::method"
     */
    private static function getCallerInfo() {
        $backtrace = debug_backtrace(DEBUG_BACKTRACE_IGNORE_ARGS, 4);
        
        // Find the first caller outside of Logger class
        foreach ($backtrace as $trace) {
            if (isset($trace['class']) && $trace['class'] !== __CLASS__) {
                $class = isset($trace['class']) ? $trace['class'] : '';
                $method = isset($trace['function']) ? $trace['function'] : '';
                
                // Remove namespace prefix for brevity
                $shortClass = basename(str_replace('\\', '/', $class));
                
                return "{$shortClass}::{$method}";
            }
        }
        
        return 'unknown';
    }
    
    /**
     * Format message with placeholders (SLF4J style)
     * Replaces {} with values from context array
     * 
     * @param string $message Message with {} placeholders
     * @param array $context Context values
     * @return string Formatted message
     */
    private static function formatMessage($message, $context) {
        // If context is empty or has no numeric keys, return message as-is
        if (empty($context) || !self::hasNumericKeys($context)) {
            return $message;
        }
        
        // Replace {} placeholders with context values
        $values = array_values($context);
        $count = 0;
        
        $formatted = preg_replace_callback('/\{\}/', function($matches) use (&$count, $values) {
            if ($count < count($values)) {
                $value = $values[$count++];
                // Convert arrays/objects to string
                if (is_array($value) || is_object($value)) {
                    return json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
                }
                return (string)$value;
            }
            return '{}';
        }, $message);
        
        return $formatted;
    }
    
    /**
     * Format context data for logging
     * Excludes numeric-indexed values (used for placeholders)
     * 
     * @param array $context Context data
     * @return string Formatted context string
     */
    private static function formatContext($context) {
        if (empty($context)) {
            return '';
        }
        
        // Filter out numeric keys (placeholder values)
        $namedContext = [];
        foreach ($context as $key => $value) {
            if (!is_int($key)) {
                $namedContext[$key] = $value;
            }
        }
        
        if (empty($namedContext)) {
            return '';
        }
        
        // Format as key=value pairs
        $pairs = [];
        foreach ($namedContext as $key => $value) {
            if (is_array($value) || is_object($value)) {
                $pairs[] = "{$key}=" . json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
            } else {
                $pairs[] = "{$key}={$value}";
            }
        }
        
        return ' ' . implode(', ', $pairs);
    }
    
    /**
     * Check if array has numeric keys (used for placeholder substitution)
     * 
     * @param array $array Array to check
     * @return bool True if array has numeric keys
     */
    private static function hasNumericKeys($array) {
        foreach ($array as $key => $value) {
            if (is_int($key)) {
                return true;
            }
        }
        return false;
    }
    
    /**
     * Set log file path
     * 
     * @param string $logFile Log file path
     * @return void
     */
    public static function setLogFile($logFile) {
        // Close current file if open
        self::close();
        self::$logFile = $logFile;
        // Reopen with new path
        if (self::$enabled) {
            self::openLogFile();
        }
    }
    
    /**
     * Enable/disable logging
     * 
     * @param bool $enabled Whether logging is enabled
     * @return void
     */
    public static function setEnabled($enabled) {
        self::$enabled = $enabled;
        if ($enabled && self::$fileHandle === null) {
            self::openLogFile();
        } elseif (!$enabled) {
            self::close();
        }
    }
    
    /**
     * Set minimum log level
     * 
     * @param int $minLevel Minimum log level
     * @return void
     */
    public static function setMinLevel($minLevel) {
        self::$minLevel = $minLevel;
    }
    
    /**
     * Set maximum log file size
     * 
     * @param int $maxFileSize Maximum file size in bytes
     * @return void
     */
    public static function setMaxFileSize($maxFileSize) {
        self::$maxFileSize = $maxFileSize;
    }
    
    /**
     * Set maximum number of backup files
     * 
     * @param int $maxBackupFiles Maximum backup files
     * @return void
     */
    public static function setMaxBackupFiles($maxBackupFiles) {
        self::$maxBackupFiles = $maxBackupFiles;
    }
    
    /**
     * Start performance timer
     * 
     * @param string $name Timer name
     * @return void
     */
    public static function startTimer($name) {
        if (!self::$enableTiming) {
            return;
        }
        self::$timers[$name] = microtime(true);
    }
    
    /**
     * Stop performance timer and log elapsed time
     * 
     * @param string $name Timer name
     * @param string $message Log message (optional, uses timer name if not provided)
     * @param array $context Additional context
     * @return float|null Elapsed time in milliseconds, or null if timer not found
     */
    public static function stopTimer($name, $message = null, $context = []) {
        if (!self::$enableTiming || !isset(self::$timers[$name])) {
            return null;
        }
        
        $startTime = self::$timers[$name];
        $elapsedMs = (microtime(true) - $startTime) * 1000;
        unset(self::$timers[$name]);
        
        if ($message === null) {
            $message = "Performance timer '{$name}' completed";
        }
        
        // Add elapsed time to context
        $context['elapsedMs'] = round($elapsedMs, 2);
        
        // Log with appropriate level based on duration
        if ($elapsedMs > 1000) {
            self::warn("{}, took {}ms", [$message, round($elapsedMs, 2)], $context);
        } else {
            self::debug("{}, took {}ms", [$message, round($elapsedMs, 2)], $context);
        }
        
        return $elapsedMs;
    }
    
    /**
     * Enable or disable performance timing
     * 
     * @param bool $enabled Whether to enable timing
     * @return void
     */
    public static function setEnableTiming($enabled) {
        self::$enableTiming = $enabled;
    }
    
    /**
     * Get all active timers
     * 
     * @return array Active timers
     */
    public static function getActiveTimers() {
        return array_keys(self::$timers);
    }
    
    /**
     * Destructor - close log file
     */
    public function __destruct() {
        self::close();
    }
}
