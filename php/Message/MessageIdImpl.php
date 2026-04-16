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
 * MessageIdImpl implements MessageId interface
 * 
 * References Java MessageIdImpl (2.0KB)
 */
class MessageIdImpl implements MessageId
{
    /**
     * @var string Version
     */
    private $version;
    
    /**
     * @var string Suffix
     */
    private $suffix;
    
    /**
     * Constructor
     * 
     * @param string $version Message ID version
     * @param string $suffix Message ID suffix
     */
    public function __construct(string $version, string $suffix)
    {
        $this->version = $version;
        $this->suffix = $suffix;
    }
    
    /**
     * {@inheritdoc}
     */
    public function getVersion(): string
    {
        return $this->version;
    }
    
    /**
     * Get suffix
     * 
     * @return string Suffix
     */
    public function getSuffix(): string
    {
        return $this->suffix;
    }
    
    /**
     * {@inheritdoc}
     */
    public function __toString(): string
    {
        return $this->version . $this->suffix;
    }
}
