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

namespace Apache\Rocketmq\Exception;

/**
 * Exception thrown when lite subscription quota is exceeded
 * 
 * This exception indicates that the number of lite topic subscriptions
 * has exceeded the configured quota limit for the consumer group.
 */
class LiteSubscriptionQuotaExceededException extends ClientException {
    /**
     * @var string The lite topic name
     */
    private $liteTopic;
    
    /**
     * Constructor
     * 
     * @param string $message Error message
     * @param string $liteTopic The lite topic that exceeded quota
     * @param int $code Error code
     * @param \Throwable|null $previous Previous exception
     */
    public function __construct(string $message = "", string $liteTopic = "", int $code = 0, \Throwable $previous = null) {
        $this->liteTopic = $liteTopic;
        parent::__construct($message, $code, $previous);
    }
    
    /**
     * Get the lite topic name
     * 
     * @return string Lite topic name
     */
    public function getLiteTopic(): string {
        return $this->liteTopic;
    }
}
