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

namespace Apache\Rocketmq\Consumer;

/**
 * RetryPolicy - Message retry strategy
 * 
 * Reference Java RetryPolicy:
 * - Controls message retry behavior on consumption failure
 * - Supports exponential backoff
 * - Configurable max attempts and backoff strategy
 */
class RetryPolicy
{
    /**
     * @var int Maximum retry attempts
     */
    private $maxAttempts;
    
    /**
     * @var int Initial backoff duration in milliseconds
     */
    private $initialBackoffMs;
    
    /**
     * @var int Maximum backoff duration in milliseconds
     */
    private $maxBackoffMs;
    
    /**
     * @var float Backoff multiplier (exponential backoff)
     */
    private $backoffMultiplier;
    
    /**
     * Constructor
     * 
     * @param int $maxAttempts Maximum retry attempts
     * @param int $initialBackoffMs Initial backoff in milliseconds
     * @param int $maxBackoffMs Maximum backoff in milliseconds
     * @param float $backoffMultiplier Backoff multiplier
     */
    public function __construct(
        int $maxAttempts = 16,
        int $initialBackoffMs = 1000,
        int $maxBackoffMs = 30000,
        float $backoffMultiplier = 2.0
    ) {
        $this->maxAttempts = $maxAttempts;
        $this->initialBackoffMs = $initialBackoffMs;
        $this->maxBackoffMs = $maxBackoffMs;
        $this->backoffMultiplier = $backoffMultiplier;
    }
    
    /**
     * Get maximum retry attempts
     * 
     * @return int
     */
    public function getMaxAttempts(): int
    {
        return $this->maxAttempts;
    }
    
    /**
     * Check if should retry
     * 
     * @param int $attempt Current attempt number (1-based)
     * @param \Exception|null $exception Exception that occurred
     * @return bool
     */
    public function shouldRetry(int $attempt, ?\Exception $exception = null): bool
    {
        if ($attempt >= $this->maxAttempts) {
            return false;
        }
        
        // Some exceptions should not be retried
        if ($exception !== null) {
            // Don't retry client configuration errors
            if ($exception instanceof \Apache\Rocketmq\Exception\ClientConfigurationException) {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * Get next attempt delay in milliseconds
     * 
     * @param int $attempt Current attempt number (1-based)
     * @return int Delay in milliseconds
     */
    public function getNextAttemptDelay(int $attempt): int
    {
        if ($attempt < 1) {
            $attempt = 1;
        }
        
        // Exponential backoff: initialBackoff * (multiplier ^ (attempt - 1))
        $delay = (int)($this->initialBackoffMs * pow($this->backoffMultiplier, $attempt - 1));
        
        // Cap at maximum backoff
        return min($delay, $this->maxBackoffMs);
    }
    
    /**
     * Get initial backoff in milliseconds
     * 
     * @return int
     */
    public function getInitialBackoffMs(): int
    {
        return $this->initialBackoffMs;
    }
    
    /**
     * Get maximum backoff in milliseconds
     * 
     * @return int
     */
    public function getMaxBackoffMs(): int
    {
        return $this->maxBackoffMs;
    }
    
    /**
     * Get backoff multiplier
     * 
     * @return float
     */
    public function getBackoffMultiplier(): float
    {
        return $this->backoffMultiplier;
    }
}
