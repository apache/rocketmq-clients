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

use Apache\Rocketmq\Logger;

/**
 * Metrics Support Trait - Provides metrics collection capabilities
 * 
 * This trait can be used by Producer, Consumer, or any class that needs metrics support.
 * It provides:
 * - MetricsCollector instance
 * - ClientMeterManager for advanced metrics management
 * - Automatic MessageMeterInterceptor integration
 * - Easy enable/disable methods
 * 
 * Usage:
 * ```php
 * class MyConsumer {
 *     use MetricsSupportTrait;
 *     
 *     public function __construct(string $clientId) {
 *         $this->initMetrics($clientId);
 *     }
 * }
 * ```
 */
trait MetricsSupportTrait {
    /**
     * @var MetricsCollector Metrics collector instance
     */
    private $metricsCollector;
    
    /**
     * @var ClientMeterManager|null Client meter manager
     */
    private $meterManager = null;
    
    /**
     * @var MessageInterceptor[] Interceptor list
     */
    private $interceptors = [];
    
    /**
     * Initialize metrics support
     * 
     * @param string $clientId Client ID
     * @return void
     */
    protected function initMetrics(string $clientId): void {
        $this->metricsCollector = new MetricsCollector($clientId);
        Logger::debug("Metrics initialized, clientId={}", [$clientId]);
    }
    
    /**
     * Enable metrics collection with automatic meter interceptor
     * 
     * @param string|null $exportEndpoint OTLP export endpoint (optional)
     * @param int $exportInterval Export interval in seconds (default: 60)
     * @return self Return current instance for chainable calls
     */
    public function enableMetrics(?string $exportEndpoint = null, int $exportInterval = 60): self
    {
        // Create meter manager if not exists
        if ($this->meterManager === null) {
            $this->meterManager = new ClientMeterManager(
                $this->getClientId(),
                $this->metricsCollector
            );
        }
        
        // Enable metrics
        $this->meterManager->enable($exportEndpoint, $exportInterval);
        
        // Check if MessageMeterInterceptor already exists
        $hasMeterInterceptor = false;
        foreach ($this->interceptors as $interceptor) {
            if ($interceptor instanceof MessageMeterInterceptor) {
                $hasMeterInterceptor = true;
                break;
            }
        }
        
        // Add MessageMeterInterceptor if not exists
        if (!$hasMeterInterceptor) {
            $consumerGroup = method_exists($this, 'getConsumerGroup') 
                ? $this->getConsumerGroup() 
                : null;
            
            $meterInterceptor = new MessageMeterInterceptor(
                $this->metricsCollector,
                $this->getClientId(),
                $consumerGroup
            );
            $this->addInterceptor($meterInterceptor);
            Logger::info("MessageMeterInterceptor added automatically, clientId={}, consumerGroup={}", [
                $this->getClientId(),
                $consumerGroup ?? 'N/A'
            ]);
        }
        
        Logger::info("Metrics enabled, clientId={}, exportEndpoint={}", [
            $this->getClientId(),
            $exportEndpoint ?? 'local-only'
        ]);
        
        return $this;
    }
    
    /**
     * Disable metrics collection
     * 
     * @return self Return current instance for chainable calls
     */
    public function disableMetrics(): self
    {
        if ($this->meterManager !== null) {
            $this->meterManager->disable();
            Logger::info("Metrics disabled, clientId={}", [$this->getClientId()]);
        }
        
        return $this;
    }
    
    /**
     * Get client meter manager
     * 
     * @return ClientMeterManager|null Meter manager or null if not enabled
     */
    public function getMeterManager(): ?ClientMeterManager
    {
        return $this->meterManager;
    }
    
    /**
     * Get metrics collector
     * 
     * @return MetricsCollector Metrics collector
     */
    public function getMetricsCollector(): MetricsCollector
    {
        return $this->metricsCollector;
    }
    
    /**
     * Shutdown metrics support
     * 
     * @return void
     */
    protected function shutdownMetrics(): void {
        if ($this->meterManager !== null) {
            $this->meterManager->shutdown();
            Logger::info("Metrics shutdown, clientId={}", [$this->getClientId()]);
        }
    }
    
    /**
     * Add message interceptor
     * 
     * @param MessageInterceptor $interceptor Interceptor instance
     * @return self Return current instance for chainable calls
     */
    public function addInterceptor(MessageInterceptor $interceptor): self
    {
        $this->interceptors[] = $interceptor;
        return $this;
    }
    
    /**
     * Remove message interceptor
     * 
     * @param MessageInterceptor $interceptor Interceptor instance
     * @return bool Whether successfully removed
     */
    public function removeInterceptor(MessageInterceptor $interceptor): bool
    {
        $index = array_search($interceptor, $this->interceptors, true);
        if ($index !== false) {
            unset($this->interceptors[$index]);
            $this->interceptors = array_values($this->interceptors);
            return true;
        }
        return false;
    }
    
    /**
     * Get interceptor list
     * 
     * @return MessageInterceptor[]
     */
    public function getInterceptors(): array
    {
        return $this->interceptors;
    }
    
    /**
     * Abstract method - must be implemented by using class
     * 
     * @return string Client ID
     */
    abstract public function getClientId(): string;
}
