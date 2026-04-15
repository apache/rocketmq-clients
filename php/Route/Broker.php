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
 * Broker represents a RocketMQ broker node
 * 
 * References Java Broker implementation (78 lines)
 */
class Broker
{
    /**
     * @var string Broker name
     */
    private $name;
    
    /**
     * @var int Broker ID (0 for master, >0 for slave)
     */
    private $id;
    
    /**
     * @var Endpoints Broker endpoints
     */
    private $endpoints;
    
    /**
     * Constructor from Protobuf
     * 
     * @param \Apache\Rocketmq\V2\Broker $broker Protobuf broker
     */
    public function __construct(\Apache\Rocketmq\V2\Broker $broker)
    {
        $this->name = $broker->getName();
        $this->id = $broker->getId();
        $this->endpoints = new Endpoints($broker->getEndpoints());
    }
    
    /**
     * Get broker name
     * 
     * @return string Broker name
     */
    public function getName(): string
    {
        return $this->name;
    }
    
    /**
     * Get broker ID
     * 
     * @return int Broker ID
     */
    public function getId(): int
    {
        return $this->id;
    }
    
    /**
     * Get broker endpoints
     * 
     * @return Endpoints Endpoints
     */
    public function getEndpoints(): Endpoints
    {
        return $this->endpoints;
    }
    
    /**
     * Convert to Protobuf
     * 
     * @return \Apache\Rocketmq\V2\Broker Protobuf broker
     */
    public function toProtobuf(): \Apache\Rocketmq\V2\Broker
    {
        $broker = new \Apache\Rocketmq\V2\Broker();
        $broker->setName($this->name);
        $broker->setId($this->id);
        $broker->setEndpoints($this->endpoints->toProtobuf());
        return $broker;
    }
    
    /**
     * {@inheritdoc}
     */
    public function __toString(): string
    {
        return "Broker{name={$this->name}, id={$this->id}, endpoints={$this->endpoints}}";
    }
}
