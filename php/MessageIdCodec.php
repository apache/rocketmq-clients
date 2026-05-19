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
 * MessageIdCodec - Message ID Codec
 * 
 * The codec for the message-id.
 * 
 * Codec here provides the following two functions:
 * 1. Provide decoding function of message-id of all versions above v0.
 * 2. Provide a generator of message-id of v1 version.
 * 
 * The message-id of versions above V1 consists of 17 bytes in total. The first two bytes represent the version
 * number. For V1, these two bytes are 0x0001.
 * 
 * V1 message id example:
 * ┌──┬────────────┬────┬────────┬────────┐
 * │01│56F7E71C361B│21BC│024CCDBE│00000000│
 * └──┴────────────┴────┴────────┴────────┘
 * 
 * V1 version message id generation rules:
 *                     process id(lower 2bytes)
 *                             ▲
 * mac address(lower 6bytes)   │   sequence number(big endian)
 *                    ▲        │          ▲ (4bytes)
 *                    │        │          │
 *              ┌─────┴─────┐ ┌┴┐ ┌───┐ ┌─┴─┐
 *       0x01+  │     6     │ │2│ │ 4 │ │ 4 │
 *              └───────────┘ └─┘ └─┬─┘ └───┘
 *                                  │
 *                                  ▼
 *           seconds since 2021-01-01 00:00:00(UTC+0)
 *                         (lower 4bytes)
 */
class MessageIdCodec
{
    const MESSAGE_ID_LENGTH_FOR_V1_OR_LATER = 34;
    const MESSAGE_ID_VERSION_V0 = '00';
    const MESSAGE_ID_VERSION_V1 = '01';

    private static $instance = null;

    private $processFixedStringV1;
    private $secondsSinceCustomEpoch;
    private $secondsStartTimestamp;
    private $seconds;
    private $sequence;

    /**
     * Private constructor (singleton pattern)
     */
    private function __construct()
    {
        // Generate process fixed string (MAC address + PID)
        $this->processFixedStringV1 = $this->generateProcessFixedString();
        
        // Calculate seconds since custom epoch (2021-01-01 00:00:00 UTC)
        $this->secondsSinceCustomEpoch = time() - $this->customEpochMillis();
        
        // Record startup timestamp
        $this->secondsStartTimestamp = hrtime(true);
        
        // Initialize current seconds
        $this->seconds = $this->deltaSeconds();
        
        // Initialize sequence number
        $this->sequence = 0;
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
     * Generate the next message ID
     *
     * @return MessageId
     */
    public function nextMessageId(): MessageId
    {
        // Calculate delta seconds
        $deltaSeconds = $this->deltaSeconds();
        if ($this->seconds !== $deltaSeconds) {
            $this->seconds = $deltaSeconds;
        }
        
        // Build buffer (4-byte seconds + 4-byte sequence number)
        $buffer = pack('NN', $deltaSeconds & 0xFFFFFFFF, $this->sequence++);
        
        // Convert to hexadecimal string
        $suffix = $this->processFixedStringV1 . strtoupper(bin2hex($buffer));
        
        return new MessageIdImpl(self::MESSAGE_ID_VERSION_V1, $suffix);
    }

    /**
     * Decode message ID string
     *
     * @param string $messageId Message ID string
     * @return MessageId
     */
    public function decode(string $messageId): MessageId
    {
        if (strlen($messageId) !== self::MESSAGE_ID_LENGTH_FOR_V1_OR_LATER) {
            return new MessageIdImpl(self::MESSAGE_ID_VERSION_V0, $messageId);
        }
        
        return new MessageIdImpl(substr($messageId, 0, 2), substr($messageId, 2));
    }

    /**
     * Generate process fixed string (MAC address + PID)
     *
     * @return string Hexadecimal string
     */
    private function generateProcessFixedString(): string
    {
        // Get MAC address (take first 6 bytes)
        $macAddress = $this->getMacAddress();
        
        // Get process ID (take lower 2 bytes)
        $pid = getmypid() & 0xFFFF;
        
        // Combine: 6-byte MAC + 2-byte PID = 8 bytes
        $buffer = $macAddress . pack('n', $pid);
        
        return strtoupper(bin2hex($buffer));
    }

    /**
     * Get MAC address
     *
     * @return string 6-byte MAC address
     */
    private function getMacAddress(): string
    {
        // Try to get MAC address from system
        $macAddress = null;
        
        // macOS / Linux
        if (PHP_OS_FAMILY === 'Darwin' || PHP_OS_FAMILY === 'Linux') {
            $output = [];
            exec('ifconfig 2>/dev/null | grep -E "ether|HWaddr" | head -n 1', $output);
            
            if (!empty($output)) {
                $line = $output[0];
                if (preg_match('/([0-9A-Fa-f]{2}[:-]){5}([0-9A-Fa-f]{2})/', $line, $matches)) {
                    $macString = str_replace([':', '-'], '', $matches[0]);
                    $macAddress = hex2bin($macString);
                }
            }
        }
        
        // Use random value if unable to obtain
        if ($macAddress === null || strlen($macAddress) < 6) {
            $macAddress = random_bytes(6);
        }
        
        // Ensure 6 bytes are returned
        return substr($macAddress, 0, 6);
    }

    /**
     * Calculate custom epoch milliseconds (2021-01-01 00:00:00 UTC)
     *
     * @return int Epoch milliseconds
     */
    private function customEpochMillis(): int
    {
        return gmmktime(0, 0, 0, 1, 1, 2021) * 1000;
    }

    /**
     * Calculate seconds since custom epoch
     *
     * @return int Number of seconds
     */
    private function deltaSeconds(): int
    {
        $nanoTime = hrtime(true);
        $elapsedSeconds = intdiv($nanoTime - $this->secondsStartTimestamp, 1000000000);
        
        return $this->secondsSinceCustomEpoch + $elapsedSeconds;
    }
}
