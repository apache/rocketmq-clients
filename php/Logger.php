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
        
        // Open file in append mode
        self::$fileHandle = fopen($logPath, 'a');
        if (self::$fileHandle === false) {
            // Fallback to error_log if file cannot be opened
            error_log("Failed to open log file: {$logPath}");
            self::$fileHandle = null;
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
     * @return void
     */
    public static function error($message, $context = []) {
        self::log(self::ERROR, $message, $context);
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
     * Destructor - close log file
     */
    public function __destruct() {
        self::close();
    }
}
