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
 * MessageIdImpl - 消息 ID 实现类（参考 Java MessageIdImpl）
 */
class MessageIdImpl implements MessageId
{
    private $version;
    private $suffix;

    /**
     * 构造函数
     *
     * @param string $version 版本号
     * @param string $suffix 后缀
     */
    public function __construct($version, $suffix)
    {
        $this->version = $version;
        $this->suffix = $suffix;
    }

    /**
     * Get the version of the message-id.
     *
     * @return string The version of message-id.
     */
    public function getVersion(): string
    {
        return $this->version;
    }

    /**
     * Get the suffix of the message-id.
     *
     * @return string The suffix of message-id.
     */
    public function getSuffix(): string
    {
        return $this->suffix;
    }

    /**
     * String-formed message id.
     *
     * @return string String-formed message id.
     */
    public function toString(): string
    {
        // Use suffix directly for V0
        if ($this->version === MessageIdCodec::MESSAGE_ID_VERSION_V0) {
            return $this->suffix;
        }
        
        return $this->version . $this->suffix;
    }

    /**
     * String conversion (magic method)
     *
     * @return string String-formed message id.
     */
    public function __toString(): string
    {
        return $this->toString();
    }

    /**
     * Check equality
     *
     * @param mixed $other Other object
     * @return bool True if equal
     */
    public function equals($other): bool
    {
        if ($this === $other) {
            return true;
        }
        
        if ($other === null || get_class($other) !== get_class($this)) {
            return false;
        }
        
        return $this->version === $other->version && $this->suffix === $other->suffix;
    }

    /**
     * Get hash code
     *
     * @return int Hash code
     */
    public function hashCode(): int
    {
        return crc32($this->version . $this->suffix);
    }
}
