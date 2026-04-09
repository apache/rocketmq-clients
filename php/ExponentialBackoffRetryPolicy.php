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

require_once __DIR__ . '/RetryPolicy.php';

/**
 * Exponential backoff retry policy
 * 
 * Refer to Java client implementation, uses exponential backoff algorithm:
 * delay = min(initialBackoff * multiplier^(attempt-1), maxBackoff)
 * 
 * Usage example:
 * $policy = new ExponentialBackoffRetryPolicy(3, 100, 5000, 2.0);
 * // 1st retry: 100ms
 * // 2nd retry: 200ms
 * // 3rd retry: 400ms (not exceeding maxBackoff)
 */
class ExponentialBackoffRetryPolicy implements RetryPolicy
{
    /**
     * @var int Maximum retry attempts
     */
    private $maxAttempts;
    
    /**
     * @var int Initial backoff time (milliseconds)
     */
    private $initialBackoff;
    
    /**
     * @var int Maximum backoff time (milliseconds)
     */
    private $maxBackoff;
    
    /**
     * @var float Backoff multiplier
     */
    private $backoffMultiplier;
    
    /**
     * Constructor
     * 
     * @param int $maxAttempts Maximum retry attempts (default 3)
     * @param int $initialBackoff Initial backoff time in milliseconds (default 100ms)
     * @param int $maxBackoff Maximum backoff time in milliseconds (default 5000ms)
     * @param float $backoffMultiplier Backoff multiplier (default 2.0)
     * @throws \InvalidArgumentException Throws exception when parameters are invalid
     */
    public function __construct(
        $maxAttempts = 3,
        $initialBackoff = 100,
        $maxBackoff = 5000,
        $backoffMultiplier = 2.0
    ) {
        if ($maxAttempts < 1) {
            throw new \InvalidArgumentException("maxAttempts must be >= 1");
        }
        if ($initialBackoff < 0) {
            throw new \InvalidArgumentException("initialBackoff must be >= 0");
        }
        if ($maxBackoff < 0) {
            throw new \InvalidArgumentException("maxBackoff must be >= 0");
        }
        if ($backoffMultiplier < 1.0) {
            throw new \InvalidArgumentException("backoffMultiplier must be >= 1.0");
        }
        
        $this->maxAttempts = $maxAttempts;
        $this->initialBackoff = $initialBackoff;
        $this->maxBackoff = $maxBackoff;
        $this->backoffMultiplier = $backoffMultiplier;
    }
    
    /**
     * Create immediate retry policy (no delay)
     * 
     * @param int $maxAttempts Maximum retry attempts
     * @return ExponentialBackoffRetryPolicy Retry policy instance
     */
    public static function immediatelyRetryPolicy($maxAttempts = 3)
    {
        return new self($maxAttempts, 0, 0, 1.0);
    }
    
    /**
     * Create fixed delay retry policy
     * 
     * @param int $maxAttempts Maximum retry attempts
     * @param int $delay Fixed delay time (milliseconds)
     * @return ExponentialBackoffRetryPolicy Retry policy instance
     */
    public static function fixedDelayRetryPolicy($maxAttempts = 3, $delay = 1000)
    {
        return new self($maxAttempts, $delay, $delay, 1.0);
    }
    
    /**
     * @inheritDoc
     */
    public function getMaxAttempts()
    {
        return $this->maxAttempts;
    }
    
    /**
     * @inheritDoc
     */
    public function getNextAttemptDelay($attempt)
    {
        if ($attempt < 1) {
            throw new \InvalidArgumentException("attempt must be >= 1");
        }
        
        // Calculate exponential backoff time
        $delayNanos = $this->initialBackoff * pow($this->backoffMultiplier, $attempt - 1);
        
        // Do not exceed maximum backoff time
        $delayNanos = min($delayNanos, $this->maxBackoff);
        
        // If calculated result <= 0, return 0
        if ($delayNanos <= 0) {
            return 0;
        }
        
        return (int)$delayNanos;
    }
    
    /**
     * @inheritDoc
     */
    public function shouldRetry($attempt, $exception)
    {
        // Check if exceeded maximum retry attempts
        if ($attempt >= $this->maxAttempts) {
            return false;
        }
        
        // Determine if exception is retryable
        return $this->isRetryableException($exception);
    }
    
    /**
     * Determine if exception is retryable
     * 
     * Retryable conditions:
     * - Network timeout
     * - Connection failure
     * - gRPC UNAVAILABLE (14)
     * - gRPC DEADLINE_EXCEEDED (4)
     * - gRPC RESOURCE_EXHAUSTED (8)
     * 
     * Non-retryable conditions:
     * - Parameter error
     * - Insufficient permissions
     * - Message format error
     * 
     * @param \Exception $exception Exception object
     * @return bool Whether retryable
     */
    private function isRetryableException($exception)
    {
        $message = strtolower($exception->getMessage());
        $code = $exception->getCode();
        
        // gRPC status code check
        $retryableCodes = [
            14, // UNAVAILABLE - Service unavailable
            4,  // DEADLINE_EXCEEDED - Timeout
            8,  // RESOURCE_EXHAUSTED - Resource exhausted (rate limiting)
            1,  // CANCELLED - Cancelled (may be temporary issue)
            10, // ABORTED - Aborted (concurrent conflict)
        ];
        
        if (in_array($code, $retryableCodes)) {
            return true;
        }
        
        // Determine based on error message
        $retryableKeywords = [
            'timeout',
            'deadline exceeded',
            'unavailable',
            'connection refused',
            'connection reset',
            'network error',
            'too many requests',
            'throttl',  // throttled/throttling
        ];
        
        foreach ($retryableKeywords as $keyword) {
            if (strpos($message, $keyword) !== false) {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Get initial backoff time
     * 
     * @return int Initial backoff time (milliseconds)
     */
    public function getInitialBackoff()
    {
        return $this->initialBackoff;
    }
    
    /**
     * Get maximum backoff time
     * 
     * @return int Maximum backoff time (milliseconds)
     */
    public function getMaxBackoff()
    {
        return $this->maxBackoff;
    }
    
    /**
     * Get backoff multiplier
     * 
     * @return float Backoff multiplier
     */
    public function getBackoffMultiplier()
    {
        return $this->backoffMultiplier;
    }
    
    /**
     * Convert to string representation
     * 
     * @return string Policy description
     */
    public function __toString()
    {
        return sprintf(
            "ExponentialBackoffRetryPolicy[maxAttempts=%d, initialBackoff=%dms, maxBackoff=%dms, multiplier=%.1f]",
            $this->maxAttempts,
            $this->initialBackoff,
            $this->maxBackoff,
            $this->backoffMultiplier
        );
    }
}
