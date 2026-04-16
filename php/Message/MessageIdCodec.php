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

namespace Apache\Rocketmq\Message;

/**
 * MessageIdCodec provides encoding and decoding for message IDs
 * 
 * References Java MessageIdCodec implementation
 * 
 * Message ID format (V1):
 * - Total: 34 characters (17 bytes in hex)
 * - Version: 2 chars (01 for V1)
 * - MAC address: 12 chars (lower 6 bytes)
 * - Process ID: 4 chars (lower 2 bytes)
 * - Timestamp: 8 chars (seconds since 2021-01-01, lower 4 bytes)
 * - Sequence: 8 chars (4 bytes, big endian)
 * 
 * Generation rules:
 * ┌──┬────────────┬────┬────────┬────────┐
 * │01│56F7E71C361B│21BC│024CCDBE│00000000│
 * └──┴────────────┴────┴────────┴────────┘
 *      ▲             ▲      ▲         ▲
 *      │             │      │         └─ Sequence (4 bytes)
 *      │             │      └─────────── Timestamp (4 bytes)
 *      │             └────────────────── Process ID (2 bytes)
 *      └─────────────────────────────── MAC address (6 bytes)
 */
class MessageIdCodec
{
    private const MESSAGE_ID_LENGTH_V1 = 34;
    public const MESSAGE_ID_VERSION_V0 = '00';
    public const MESSAGE_ID_VERSION_V1 = '01';
    
    private static ?MessageIdCodec $instance = null;
    
    private string $processFixedStringV1;
    private int $secondsSinceCustomEpoch;
    private int $secondsStartTimestamp;
    private int $seconds;
    private int $sequence = 0;
    
    /**
     * Private constructor for singleton
     */
    private function __construct()
    {
        $mac = $this->getMacAddress();
        $pid = getmypid() & 0xFFFF;
        
        $this->processFixedStringV1 = sprintf('%s%04x', $mac, $pid);
        
        $customEpochMillis = $this->customEpochMillis();
        $this->secondsSinceCustomEpoch = (int)((microtime(true) * 1000 - $customEpochMillis) / 1000);
        $this->secondsStartTimestamp = (int)(hrtime(true) / 1000000000);
        $this->seconds = $this->deltaSeconds();
    }
    
    /**
     * Get singleton instance
     */
    public static function getInstance(): MessageIdCodec
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }
    
    /**
     * Custom epoch: 2021-01-01 00:00:00 UTC
     */
    private function customEpochMillis(): int
    {
        return gmmktime(0, 0, 0, 1, 1, 2021) * 1000;
    }
    
    /**
     * Calculate delta seconds since start using monotonic clock
     */
    private function deltaSeconds(): int
    {
        $currentSeconds = (int)(hrtime(true) / 1000000000);
        return $this->secondsSinceCustomEpoch + ($currentSeconds - $this->secondsStartTimestamp);
    }
    
    /**
     * Generate next message ID (V1)
     */
    public function nextMessageId(): MessageId
    {
        $deltaSeconds = $this->deltaSeconds();
        if ($this->seconds !== $deltaSeconds) {
            $this->seconds = $deltaSeconds;
        }
        
        $timestamp = sprintf('%08x', $this->seconds & 0xFFFFFFFF);
        $sequence = sprintf('%08x', $this->sequence);
        $this->sequence = ($this->sequence + 1) & 0xFFFFFFFF;
        
        $suffix = $this->processFixedStringV1 . $timestamp . $sequence;
        
        return new MessageIdImpl(self::MESSAGE_ID_VERSION_V1, $suffix);
    }
    
    /**
     * Decode message ID from string
     */
    public function decode(string $messageId): MessageId
    {
        if (strlen($messageId) !== self::MESSAGE_ID_LENGTH_V1) {
            return new MessageIdImpl(self::MESSAGE_ID_VERSION_V0, $messageId);
        }
        
        $version = substr($messageId, 0, 2);
        $suffix = substr($messageId, 2);
        
        return new MessageIdImpl($version, $suffix);
    }
    
    /**
     * Get MAC address (lower 6 bytes in hex)
     *
     * Uses net_get_interfaces() when available (PHP sockets extension),
     * falls back to random_bytes() matching Java's SecureRandom fallback.
     */
    private function getMacAddress(): string
    {
        // Primary: use PHP's net_get_interfaces() API (sockets extension)
        if (function_exists('net_get_interfaces')) {
            $interfaces = net_get_interfaces();
            if (is_array($interfaces)) {
                foreach ($interfaces as $name => $info) {
                    if ($name === 'lo' || $name === 'lo0') {
                        continue;
                    }
                    if (isset($info['mac']) && $info['mac'] !== '00:00:00:00:00:00') {
                        $mac = str_replace(':', '', $info['mac']);
                        if (strlen($mac) === 12 && $mac !== '000000000000') {
                            return strtolower($mac);
                        }
                    }
                }
            }
        }
        
        // Fallback: cryptographically secure random bytes (matches Java SecureRandom)
        return bin2hex(random_bytes(6));
    }
}
