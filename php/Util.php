<?php
declare(strict_types=1);

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
 * Utility class for common functions
 */
class Util {
    /**
     * Generate random string
     * 
     * @param int $length String length
     * @return string Random string
     */
    public static function getRandStr($length) {
        $str = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
        $len = strlen($str) - 1;
        $randstr = '';
        for ($i = 0; $i < $length; $i++) {
            $num = mt_rand(0, $len);
            $randstr .= $str[$num];
        }
        return $randstr;
    }
    
    /**
     * Generate client ID
     * 
     * @return string Client ID
     */
    public static function generateClientId() {
        $hostname = gethostname() ?: 'unknown';
        $pid = getmypid() ?: 0;
        $randomId = self::getRandStr(10);
        return "{$hostname}@{$pid}@{$randomId}";
    }
    
    /**
     * Calculate elapsed time in milliseconds
     * 
     * @param float $startTime Start time (microtime(true))
     * @param float $endTime End time (microtime(true))
     * @return float Elapsed time in milliseconds
     */
    public static function getElapsedTime($startTime, $endTime = null) {
        if ($endTime === null) {
            $endTime = microtime(true);
        }
        return ($endTime - $startTime) * 1000;
    }
    
    /**
     * Format array as string
     * 
     * @param array $array Array to format
     * @param int $indent Indent level
     * @return string Formatted string
     */
    public static function formatArray(array $array, $indent = 0) {
        $result = [];
        $prefix = str_repeat('  ', $indent);
        
        foreach ($array as $key => $value) {
            if (is_array($value)) {
                $result[] = "{$prefix}{$key}: [";
                $result[] = self::formatArray($value, $indent + 1);
                $result[] = "{$prefix}]";
            } else {
                $result[] = "{$prefix}{$key}: {$value}";
            }
        }
        
        return implode("\n", $result);
    }
    
    /**
     * Validate topic name
     * 
     * @param string $topic Topic name
     * @return bool Whether valid
     */
    public static function validateTopic($topic) {
        if (empty($topic)) {
            return false;
        }
        
        if (strlen($topic) > 255) {
            return false;
        }
        
        // Topic name can only contain letters, numbers, hyphens, underscores, and periods
        return preg_match('/^[a-zA-Z0-9\-_\.]+$/', $topic) === 1;
    }
    
    /**
     * Validate consumer group name
     * 
     * @param string $consumerGroup Consumer group name
     * @return bool Whether valid
     */
    public static function validateConsumerGroup($consumerGroup) {
        if (empty($consumerGroup)) {
            return false;
        }
        
        if (strlen($consumerGroup) > 255) {
            return false;
        }
        
        // Consumer group name can only contain letters, numbers, hyphens, underscores, and periods
        return preg_match('/^[a-zA-Z0-9\-_\.]+$/', $consumerGroup) === 1;
    }
    
    /**
     * Parse endpoint string into array
     * 
     * @param string $endpoints Endpoint string, format: host:port or host1:port1;host2:port2
     * @return array Endpoint array
     */
    public static function parseEndpoints($endpoints) {
        if (empty($endpoints)) {
            return [];
        }
        
        // Remove http:// or https:// prefix
        $endpoints = preg_replace('#^https?://#', '', $endpoints);
        
        // Split by semicolon
        $parts = explode(';', $endpoints);
        
        // Filter empty parts
        $parts = array_filter($parts, function($part) {
            return trim($part) !== '';
        });
        
        return array_values($parts);
    }
    
    /**
     * Get current timestamp in milliseconds
     * 
     * @return int Timestamp in milliseconds
     */
    public static function getCurrentTimestamp(): int {
        return (int)(microtime(true) * 1000);
    }
    
    /**
     * Calculate CRC32 checksum of data and return as uppercase hex string
     * 
     * Matches Java Utilities.crc32CheckSum() output format.
     * 
     * @param string $data Data to checksum
     * @return string 8-character uppercase hex CRC32 checksum
     */
    public static function crc32Checksum(string $data): string {
        return sprintf('%08X', crc32($data) & 0xffffffff);
    }
    
    /**
     * Get SDK version from composer.json or fallback to constant
     * 
     * Similar to Java MetadataUtils.getVersion()
     * 
     * @return string SDK version
     */
    public static function getSdkVersion(): string {
        static $sdkVersion = null;
        
        if ($sdkVersion !== null) {
            return $sdkVersion;
        }
        
        // Try reading from composer.json
        $composerFile = __DIR__ . '/composer.json';
        if (file_exists($composerFile)) {
            $composerData = @json_decode((string)file_get_contents($composerFile), true);
            if (is_array($composerData) && isset($composerData['version']) && is_string($composerData['version'])) {
                $sdkVersion = $composerData['version'];
                return $sdkVersion;
            }
        }
        
        // Fallback
        $sdkVersion = '5.0.0';
        return $sdkVersion;
    }
}
