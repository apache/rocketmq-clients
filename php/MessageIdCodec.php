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
 * MessageIdCodec - 消息 ID 编解码器（参考 Java MessageIdCodec）
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
     * 私有构造函数（单例模式）
     */
    private function __construct()
    {
        // 生成进程固定字符串（MAC 地址 + PID）
        $this->processFixedStringV1 = $this->generateProcessFixedString();
        
        // 计算自定义纪元以来的秒数（2021-01-01 00:00:00 UTC）
        $this->secondsSinceCustomEpoch = time() - $this->customEpochMillis();
        
        // 记录启动时间戳
        $this->secondsStartTimestamp = hrtime(true);
        
        // 初始化当前秒数
        $this->seconds = $this->deltaSeconds();
        
        // 初始化序列号
        $this->sequence = 0;
    }

    /**
     * 获取单例实例
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
     * 生成下一个消息 ID
     *
     * @return MessageId
     */
    public function nextMessageId(): MessageId
    {
        // 计算增量秒数
        $deltaSeconds = $this->deltaSeconds();
        if ($this->seconds !== $deltaSeconds) {
            $this->seconds = $deltaSeconds;
        }
        
        // 构建缓冲区（4字节秒数 + 4字节序列号）
        $buffer = pack('NN', $deltaSeconds & 0xFFFFFFFF, $this->sequence++);
        
        // 转换为十六进制字符串
        $suffix = $this->processFixedStringV1 . strtoupper(bin2hex($buffer));
        
        return new MessageIdImpl(self::MESSAGE_ID_VERSION_V1, $suffix);
    }

    /**
     * 解码消息 ID 字符串
     *
     * @param string $messageId 消息 ID 字符串
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
     * 生成进程固定字符串（MAC 地址 + PID）
     *
     * @return string 十六进制字符串
     */
    private function generateProcessFixedString(): string
    {
        // 获取 MAC 地址（取前 6 字节）
        $macAddress = $this->getMacAddress();
        
        // 获取进程 ID（取低 2 字节）
        $pid = getmypid() & 0xFFFF;
        
        // 组合：6 字节 MAC + 2 字节 PID = 8 字节
        $buffer = $macAddress . pack('n', $pid);
        
        return strtoupper(bin2hex($buffer));
    }

    /**
     * 获取 MAC 地址
     *
     * @return string 6 字节的 MAC 地址
     */
    private function getMacAddress(): string
    {
        // 尝试从系统获取 MAC 地址
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
        
        // 如果无法获取，使用随机值
        if ($macAddress === null || strlen($macAddress) < 6) {
            $macAddress = random_bytes(6);
        }
        
        // 确保返回 6 字节
        return substr($macAddress, 0, 6);
    }

    /**
     * 计算自定义纪元毫秒数（2021-01-01 00:00:00 UTC）
     *
     * @return int 纪元毫秒数
     */
    private function customEpochMillis(): int
    {
        return gmmktime(0, 0, 0, 1, 1, 2021) * 1000;
    }

    /**
     * 计算自自定义纪元以来的秒数
     *
     * @return int 秒数
     */
    private function deltaSeconds(): int
    {
        $nanoTime = hrtime(true);
        $elapsedSeconds = intdiv($nanoTime - $this->secondsStartTimestamp, 1000000000);
        
        return $this->secondsSinceCustomEpoch + $elapsedSeconds;
    }
}
