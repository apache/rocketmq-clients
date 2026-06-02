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
 * Example Configuration - Centralized configuration for all examples
 * 
 * This file provides unified configuration parameters for all PHP examples.
 * Configuration can be set via environment variables or defaults.
 * 
 * Usage:
 *   require_once __DIR__ . '/ExampleConfig.php';
 *   $config = ExampleConfig::getInstance();
 *   
 *   // Access configuration
 *   $endpoints = $config->getEndpoints();
 *   $topic = $config->getTopic('normal');
 *   $credentials = $config->getCredentials();
 */

use Apache\Rocketmq\TlsCredentials;

require_once __DIR__ . '/../SessionCredentials.php';
require_once __DIR__ . '/../TlsCredentials.php';

class ExampleConfig
{
    private static $instance = null;
    
    // Connection settings
    private $endpoints;
    private $namespace;
    
    // Topic configuration
    private $topics;
    
    // Consumer configuration
    private $consumerGroup;
    private $tag;
    
    // Credentials
    private $credentials;
    
    // Lite consumer configuration
    private $liteTopicConfig;
    private $tlsCaCert;
    private $tlsClientCent;
    private $tlsClientKey;
    
    /**
     * Private constructor - use getInstance() instead
     */
    private function __construct()
    {
        // Load from environment variables or use defaults
        $this->endpoints = getenv('ROCKETMQ_PHP_CLIENT_ENDPOINTS') ?: '127.0.0.1:8081';
        $this->namespace = getenv('ROCKETMQ_PHP_CLIENT_NAMESPACE') ?: '';
        
        // Topic configuration
        $this->topics = [
            'normal' => getenv('ROCKETMQ_PHP_TOPIC_NORMAL') ?: 'TopicTestForNormal',
            'fifo' => getenv('ROCKETMQ_PHP_TOPIC_FIFO') ?: 'FifoTestTopic',
            'delay' => getenv('ROCKETMQ_PHP_TOPIC_DELAY') ?: 'DelayTestTopic',
            'transaction' => getenv('ROCKETMQ_PHP_TOPIC_TRANSACTION') ?: 'TopicTestForTransaction',
            'priority' => getenv('ROCKETMQ_PHP_TOPIC_PRIORITY') ?: 'PriorityTestTopic',
        ];
        
        // Consumer configuration
        $this->consumerGroup = getenv('ROCKETMQ_PHP_CLIENT_GROUP') ?: 'GID_DefaultConsumer';
        $this->tag = getenv('ROCKETMQ_PHP_TAG') ?: '*';
        
        // Credentials (optional)
        $accessKey = getenv('ROCKETMQ_PHP_CLIENT_KEY') ?: '';
        $secretKey = getenv('ROCKETMQ_PHP_CLIENT_SECRET') ?: '';
        
        if (!empty($accessKey) && !empty($secretKey)) {
            $this->credentials = new SessionCredentials($accessKey, $secretKey);
        } else {
            $this->credentials = null;
        }
        
        // Lite topic configuration
        $this->liteTopicConfig = [
            'parentTopic' => getenv('ROCKETMQ_PHP_LITE_PARENT_TOPIC') ?: 'yourParentTopic',
        ];
        // TLS configuration
        $this->tlsCaCert = getenv('ROCKETMQ_PHP_TLS_CA_CERT') ?: null;
        $this->tlsClientCent = getenv('ROCKETMQ_PHP_TLS_CLIENT_CERT') ?: null;
        $this->tlsClientKey = getenv('ROCKETMQ_PHP_TLS_CLIENT_KEY') ?: null;
    }

    /**
     * Get TLS credentials
     * @return void
     */
    public function getTlsCredentials()
    {
        if (!empty($this->tlsClientCent) && !empty($this->tlsClientKey)) {
            return TlsCredentials::createMtls($this->tlsCaCert, $this->tlsClientCent, $this->tlsClientKey);
        }

        if (!empty($this->tlsCaCert)) {
            return TlsCredentials::createWithCa($this->tlsCaCert);
        }
        return null;
    }
    
    /**
     * Get singleton instance
     * 
     * @return ExampleConfig
     */
    public static function getInstance()
    {
        if (self::$instance === null) {
            self::$instance = new self();
        }
        return self::$instance;
    }
    
    /**
     * Reset instance (useful for testing)
     */
    public static function reset()
    {
        self::$instance = null;
    }
    
    // Getters
    
    /**
     * Get endpoints
     * 
     * @return string
     */
    public function getEndpoints()
    {
        return $this->endpoints;
    }
    
    /**
     * Get namespace
     * 
     * @return string
     */
    public function getNamespace()
    {
        return $this->namespace;
    }
    
    /**
     * Get topic by type
     * 
     * @param string $type Topic type: normal, fifo, delay, transaction, priority
     * @return string
     */
    public function getTopic($type = 'normal')
    {
        return isset($this->topics[$type]) ? $this->topics[$type] : $this->topics['normal'];
    }
    
    /**
     * Get all topics
     * 
     * @return array
     */
    public function getTopics()
    {
        return $this->topics;
    }
    
    /**
     * Get consumer group
     * 
     * @return string
     */
    public function getConsumerGroup()
    {
        return $this->consumerGroup;
    }
    
    /**
     * Get tag filter expression
     * 
     * @return string
     */
    public function getTag()
    {
        return $this->tag;
    }
    
    /**
     * Get credentials (may be null)
     * 
     * @return SessionCredentials|null
     */
    public function getCredentials()
    {
        return $this->credentials;
    }
    
    /**
     * Check if credentials are configured
     * 
     * @return bool
     */
    public function hasCredentials()
    {
        return $this->credentials !== null;
    }
    
    /**
     * Get lite topic configuration
     * 
     * @return array
     */
    public function getLiteTopicConfig()
    {
        return $this->liteTopicConfig;
    }
    
    /**
     * Get lite parent topic
     * 
     * @return string
     */
    public function getLiteParentTopic()
    {
        return $this->liteTopicConfig['parentTopic'];
    }
    
    /**
     * Display current configuration (for debugging)
     */
    public function display()
    {
        echo "========================================\n";
        echo "RocketMQ PHP Client Configuration\n";
        echo "========================================\n";
        echo "Endpoints: {$this->endpoints}\n";
        echo "Namespace: " . ($this->namespace ?: '(empty)') . "\n";
        echo "Consumer Group: {$this->consumerGroup}\n";
        echo "Tag: {$this->tag}\n";
        echo "Credentials: " . ($this->hasCredentials() ? 'Configured' : 'Not configured') . "\n";
        echo "\nTopics:\n";
        foreach ($this->topics as $type => $topic) {
            echo "  {$type}: {$topic}\n";
        }
        echo "\nLite Topic:\n";
        echo "  Parent Topic: {$this->liteTopicConfig['parentTopic']}\n";
        echo "  TLS: " . ($this->tlsCaCert ? 'Enabled' : 'Disabled') . "\n";
        echo "  TLS CA Cert: " . ($this->tlsCaCert ? 'Configured' : 'Not configured') . "\n";
        echo "  TLS Client Key: ". ($this->tlsClientKey ?? 'Not configured') . "\n";
        echo "========================================\n";
    }
}
