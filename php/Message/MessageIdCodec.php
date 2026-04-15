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

namespace Apache\Rocketmq\Message;

/**
 * MessageIdCodec provides encoding and decoding for message IDs
 * 
 * References Java MessageIdCodec implementation (150 lines)
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
    const MESSAGE_ID_LENGTH_V1 = 34;
    const MESSAGE_ID_VERSION_V0 = '00';
    const MESSAGE_ID_VERSION_V1 = '01';
    
    /**
     * @var MessageIdCodec Singleton instance
     */
    private static $instance = null;
    
    /**
     * @var string Fixed prefix (MAC + PID)
     */
    private $processFixedStringV1;
    
    /**
     * @var int Seconds since custom epoch
     */
    private $secondsSinceCustomEpoch;
    
    /**
     * @var int Start timestamp
     */
    private $secondsStartTimestamp;
    
    /**
     * @var int Current seconds
     */
    private $seconds;
    
    /**
     * @var int Sequence counter
     */
    private $sequence = 0;
    
    /**
     * Private constructor for singleton
     */
    private function __construct()
    {
        // Generate fixed prefix from MAC address and PID
        $mac = $this->getMacAddress();
        $pid = getmypid() & 0xFFFF; // Lower 2 bytes
        
        $this->processFixedStringV1 = sprintf('%s%04x', $mac, $pid);
        
        // Initialize timestamp
        $customEpochMillis = $this->customEpochMillis();
        $this->secondsSinceCustomEpoch = (int)((microtime(true) * 1000 - $customEpochMillis) / 1000);
        $this->secondsStartTimestamp = (int)hrtime(true) / 1000000000;
        $this->seconds = $this->deltaSeconds();
    }
    
    /**
     * Get singleton instance
     * 
     * @return MessageIdCodec
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
     * 
     * @return int Custom epoch in milliseconds
     */
    private function customEpochMillis(): int
    {
        return gmmktime(0, 0, 0, 1, 1, 2021) * 1000;
    }
    
    /**
     * Calculate delta seconds since start
     * 
     * @return int Delta seconds
     */
    private function deltaSeconds(): int
    {
        $currentSeconds = (int)hrtime(true) / 1000000000;
        return $this->secondsSinceCustomEpoch + ($currentSeconds - $this->secondsStartTimestamp);
    }
    
    /**
     * Generate next message ID (V1)
     * 
     * @return MessageId Message ID
     */
    public function nextMessageId(): MessageId
    {
        $deltaSeconds = $this->deltaSeconds();
        if ($this->seconds !== $deltaSeconds) {
            $this->seconds = $deltaSeconds;
        }
        
        // Format: version(2) + mac(12) + pid(4) + timestamp(8) + sequence(8)
        $timestamp = sprintf('%08x', $this->seconds & 0xFFFFFFFF);
        $sequence = sprintf('%08x', $this->sequence++);
        
        $suffix = $this->processFixedStringV1 . $timestamp . $sequence;
        
        return new MessageIdImpl(self::MESSAGE_ID_VERSION_V1, $suffix);
    }
    
    /**
     * Decode message ID from string
     * 
     * @param string $messageId Message ID string
     * @return MessageId Message ID
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
     * @return string MAC address in hex
     */
    private function getMacAddress(): string
    {
        // Try to get MAC address from system
        $mac = '000000000000'; // Default
        
        // Linux
        if (PHP_OS === 'Linux' || PHP_OS === 'Linux2') {
            $output = @shell_exec('cat /sys/class/net/eth0/address 2>/dev/null');
            if ($output) {
                $mac = str_replace(':', '', trim($output));
            }
        }
        // macOS
        elseif (PHP_OS === 'Darwin') {
            $output = @shell_exec('ifconfig en0 | grep ether | awk \'{print $2}\' 2>/dev/null');
            if ($output) {
                $mac = str_replace(':', '', trim($output));
            }
        }
        
        // Ensure 12 characters
        return str_pad(strtolower($mac), 12, '0', STR_PAD_LEFT);
    }
}
