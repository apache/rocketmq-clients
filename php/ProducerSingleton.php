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

/**
 * Producer Singleton Pattern
 * 
 * Each client will establish an independent connection to the server node within a process.
 * 
 * In most cases, the singleton mode can meet the requirements of higher concurrency.
 * If multiple connections are desired, consider increasing the number of clients appropriately.
 * 
 * Usage example:
 * ```php
 * // Get normal producer instance
 * $producer = ProducerSingleton::getInstance('yourTopic');
 * 
 * // Get transactional producer instance
 * $transactionChecker = function($message) {
 *     // Check transaction status
 *     return TransactionResolution::COMMIT;
 * };
 * $producer = ProducerSingleton::getTransactionalInstance($transactionChecker, 'yourTopic');
 * ```
 */
class ProducerSingleton
{
    /**
     * @var Producer|null Normal producer instance
     */
    private static $producer = null;
    
    /**
     * @var Producer|null Transactional producer instance
     */
    private static $transactionalProducer = null;
    
    /**
     * Configuration constants
     */
    const ACCESS_KEY = 'yourAccessKey';
    const SECRET_KEY = 'yourSecretKey';
    const ENDPOINTS = '127.0.0.1:8080';
    
    /**
     * Private constructor to prevent instantiation
     */
    private function __construct()
    {
    }
    
    /**
     * Build producer instance
     * 
     * @param TransactionChecker|null $checker Transaction checker (null for normal producer)
     * @param string ...$topics Topic names
     * @return Producer Producer instance
     * @throws \Exception If build fails
     */
    private static function buildProducer($checker, ...$topics)
    {
        // Create client configuration
        $clientConfig = new \Apache\Rocketmq\ClientConfiguration(self::ENDPOINTS);
        
        // Set credentials if needed (optional, only when ACL is enabled)
        if (!empty(self::ACCESS_KEY) && !empty(self::SECRET_KEY)) {
            $sessionCredentialsProvider = new \Apache\Rocketmq\StaticSessionCredentialsProvider(
                self::ACCESS_KEY,
                self::SECRET_KEY
            );
            $clientConfig->withCredentialsProvider($sessionCredentialsProvider);
        }
        
        // Disable SSL for local development (enable in production if needed)
        $clientConfig->withSslEnabled(false);
        
        // Build producer using ProducerBuilder
        $builder = new \Apache\Rocketmq\Builder\ProducerBuilder();
        $builder->setClientConfiguration($clientConfig);
        
        // Set topics (optional but recommended for prefetching route)
        if (!empty($topics)) {
            $builder->setTopics(...$topics);
        }
        
        // Set transaction checker if provided
        if ($checker !== null) {
            $builder->setTransactionChecker($checker);
        }
        
        return $builder->build();
    }
    
    /**
     * Get normal producer instance (singleton)
     * 
     * Uses double-checked locking pattern for thread safety.
     * 
     * @param string ...$topics Topic names
     * @return Producer Producer instance
     * @throws \Exception If initialization fails
     */
    public static function getInstance(...$topics)
    {
        if (self::$producer === null) {
            // Use Swoole lock if available for better concurrency control
            if (extension_loaded('swoole')) {
                $lock = new \Swoole\Lock(SWOOLE_MUTEX);
                $lock->lock();
                try {
                    if (self::$producer === null) {
                        self::$producer = self::buildProducer(null, ...$topics);
                    }
                } finally {
                    $lock->unlock();
                }
            } else {
                // Fallback for non-Swoole environments
                if (self::$producer === null) {
                    self::$producer = self::buildProducer(null, ...$topics);
                }
            }
        }
        
        return self::$producer;
    }
    
    /**
     * Get transactional producer instance (singleton)
     * 
     * Uses double-checked locking pattern for thread safety.
     * 
     * @param TransactionChecker $checker Transaction checker callback
     * @param string ...$topics Topic names
     * @return Producer Transactional producer instance
     * @throws \Exception If initialization fails
     */
    public static function getTransactionalInstance($checker, ...$topics)
    {
        if (self::$transactionalProducer === null) {
            // Use Swoole lock if available for better concurrency control
            if (extension_loaded('swoole')) {
                $lock = new \Swoole\Lock(SWOOLE_MUTEX);
                $lock->lock();
                try {
                    if (self::$transactionalProducer === null) {
                        self::$transactionalProducer = self::buildProducer($checker, ...$topics);
                    }
                } finally {
                    $lock->unlock();
                }
            } else {
                // Fallback for non-Swoole environments
                if (self::$transactionalProducer === null) {
                    self::$transactionalProducer = self::buildProducer($checker, ...$topics);
                }
            }
        }
        
        return self::$transactionalProducer;
    }
    
    /**
     * Reset singleton instances (useful for testing)
     * 
     * @return void
     */
    public static function reset()
    {
        self::$producer = null;
        self::$transactionalProducer = null;
    }
}
