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

namespace Apache\Rocketmq\Route;

/**
 * Address represents a network address (host:port)
 * 
 * References Java Address implementation
 */
class Address
{
    /**
     * @var string Host name or IP address
     */
    private $host;
    
    /**
     * @var int Port number
     */
    private $port;
    
    /**
     * Constructor
     * 
     * @param string $host Host name or IP address
     * @param int $port Port number
     */
    public function __construct(string $host, int $port)
    {
        $this->host = $host;
        $this->port = $port;
    }
    
    /**
     * Create Address from Protobuf
     * 
     * @param \Apache\Rocketmq\V2\Address $address Protobuf address
     * @return Address
     */
    public static function fromProtobuf(\Apache\Rocketmq\V2\Address $address): Address
    {
        return new self($address->getHost(), $address->getPort());
    }
    
    /**
     * Get host
     * 
     * @return string Host
     */
    public function getHost(): string
    {
        return $this->host;
    }
    
    /**
     * Get port
     * 
     * @return int Port
     */
    public function getPort(): int
    {
        return $this->port;
    }
    
    /**
     * Get address string (host:port)
     * 
     * @return string Address
     */
    public function getAddress(): string
    {
        return $this->host . ':' . $this->port;
    }
    
    /**
     * Convert to Protobuf
     * 
     * @return \Apache\Rocketmq\V2\Address Protobuf address
     */
    public function toProtobuf(): \Apache\Rocketmq\V2\Address
    {
        $address = new \Apache\Rocketmq\V2\Address();
        $address->setHost($this->host);
        $address->setPort($this->port);
        return $address;
    }
    
    /**
     * {@inheritdoc}
     */
    public function __toString(): string
    {
        return $this->getAddress();
    }
}
