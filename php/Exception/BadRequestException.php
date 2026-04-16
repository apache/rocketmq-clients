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

namespace Apache\Rocketmq\Exception;

/**
 * Exception for bad request - indicates missing or invalid required fields
 * 
 * Reference: Java BadRequestException
 */
class BadRequestException extends ClientException {
    /**
     * @var string Request ID from server response
     */
    private $requestId;
    
    /**
     * @var int Response code from server
     */
    private $responseCode;
    
    /**
     * BadRequestException constructor
     *
     * @param int $responseCode Response code from server
     * @param string $requestId Request ID from server
     * @param string $message Error message
     * @param \Exception|null $previous Previous exception
     */
    public function __construct(int $responseCode, string $requestId, string $message, \Throwable $previous = null) {
        $this->responseCode = $responseCode;
        $this->requestId = $requestId;
        parent::__construct($message, $responseCode, $previous);
    }
    
    /**
     * Get request ID
     * 
     * @return string Request ID
     */
    public function getRequestId(): string {
        return $this->requestId;
    }
    
    /**
     * Get response code
     * 
     * @return int Response code
     */
    public function getResponseCode(): int {
        return $this->responseCode;
    }
}
