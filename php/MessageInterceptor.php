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
 * Message Interceptor Context
 * 
 * Contains information required for interceptor execution
 */
class MessageInterceptorContext
{
    /**
     * @var string Hook point type
     */
    private $hookPoint;
    
    /**
     * @var string Interceptor status
     */
    private $status;
    
    /**
     * @var array Additional attributes
     */
    private $attributes = [];
    
    /**
     * @var mixed Interceptor execution result
     */
    private $result;
    
    /**
     * @var \Exception|null Exception information
     */
    private $exception;
    
    /**
     * Constructor
     * 
     * @param string $hookPoint Hook point
     * @param string $status Status
     */
    public function __construct($hookPoint, $status = 'OK')
    {
        $this->hookPoint = $hookPoint;
        $this->status = $status;
    }
    
    /**
     * Get hook point
     * 
     * @return string
     */
    public function getHookPoint()
    {
        return $this->hookPoint;
    }
    
    /**
     * Get status
     * 
     * @return string
     */
    public function getStatus()
    {
        return $this->status;
    }
    
    /**
     * Set status
     * 
     * @param string $status
     * @return void
     */
    public function setStatus($status)
    {
        $this->status = $status;
    }
    
    /**
     * Add attribute
     * 
     * @param string $key Attribute key
     * @param mixed $value Attribute value
     * @return void
     */
    public function putAttribute($key, $value)
    {
        $this->attributes[$key] = $value;
    }
    
    /**
     * Get attribute
     * 
     * @param string $key Attribute key
     * @return mixed|null
     */
    public function getAttribute($key)
    {
        return isset($this->attributes[$key]) ? $this->attributes[$key] : null;
    }
    
    /**
     * Get all attributes
     * 
     * @return array
     */
    public function getAttributes()
    {
        return $this->attributes;
    }
    
    /**
     * Set execution result
     * 
     * @param mixed $result
     * @return void
     */
    public function setResult($result)
    {
        $this->result = $result;
    }
    
    /**
     * Get execution result
     * 
     * @return mixed
     */
    public function getResult()
    {
        return $this->result;
    }
    
    /**
     * Set exception
     * 
     * @param \Exception|null $exception
     * @return void
     */
    public function setException($exception)
    {
        $this->exception = $exception;
    }
    
    /**
     * Get exception
     * 
     * @return \Exception|null
     */
    public function getException()
    {
        return $this->exception;
    }
    
    /**
     * Convert to array
     * 
     * @return array
     */
    public function toArray()
    {
        return [
            'hookPoint' => $this->hookPoint,
            'status' => $this->status,
            'attributes' => $this->attributes,
            'result' => $this->result,
            'exception' => $this->exception ? $this->exception->getMessage() : null,
        ];
    }
}

/**
 * Message Hook Points Enum
 */
class MessageHookPoints
{
    const SEND = 'SEND';
    const RECEIVE = 'RECEIVE';
    const CONSUME = 'CONSUME';
    const ACK = 'ACK';
    const HEARTBEAT = 'HEARTBEAT';
    const TRANSACTION_COMMIT = 'TRANSACTION_COMMIT';
    const TRANSACTION_ROLLBACK = 'TRANSACTION_ROLLBACK';
}

/**
 * Message Hook Points Status Enum
 */
class MessageHookPointsStatus
{
    const OK = 'OK';
    const ERROR = 'ERROR';
    const TIMEOUT = 'TIMEOUT';
    const INVALID = 'INVALID';
}

/**
 * Message Interceptor Interface
 * 
 * Provides interception points before/after sending/receiving messages, used for implementing logging, monitoring, data masking, etc.
 * 
 * Usage example:
 * class LoggingInterceptor implements MessageInterceptor
 * {
 *     public function doBefore($context, $messages)
 *     {
 *         echo "Before send: " . count($messages) . " messages\n";
 *     }
 *     
 *     public function doAfter($context, $messages)
 *     {
 *         echo "After send: " . $context->getStatus() . "\n";
 *     }
 * }
 */
interface MessageInterceptor
{
    /**
     * Execute before sending/receiving messages
     * 
     * @param MessageInterceptorContext $context Interceptor context
     * @param array $messages Message array
     * @return void
     */
    public function doBefore($context, $messages);
    
    /**
     * Execute after sending/receiving messages
     * 
     * @param MessageInterceptorContext $context Interceptor context
     * @param array $messages Message array
     * @return void
     */
    public function doAfter($context, $messages);
}
