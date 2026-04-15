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
 * Client Meter Manager - Manages metrics collection and export
 * 
 * This manager controls:
 * - Enable/disable metrics collection
 * - Configure metric endpoints
 * - Manage gauge observers
 * - Record histogram metrics
 * 
 * Reference: Java ClientMeterManager
 */
class ClientMeterManager {
    /**
     * @var string Client ID
     */
    private $clientId;
    
    /**
     * @var MetricsCollector Metrics collector instance
     */
    private $metricsCollector;
    
    /**
     * @var bool Whether metrics is enabled
     */
    private $enabled = false;
    
    /**
     * @var string|null Metric export endpoint
     */
    private $exportEndpoint = null;
    
    /**
     * @var int Export interval in seconds (default: 60s)
     */
    private $exportInterval = 60;
    
    /**
     * @var callable|null Gauge observer callback
     */
    private $gaugeObserver = null;
    
    /**
     * @var array Registered gauges
     */
    private $gauges = [];
    
    /**
     * Constructor
     * 
     * @param string $clientId Client ID
     * @param MetricsCollector $metricsCollector Metrics collector
     */
    public function __construct(string $clientId, MetricsCollector $metricsCollector) {
        $this->clientId = $clientId;
        $this->metricsCollector = $metricsCollector;
        
        Logger::info("ClientMeterManager initialized, clientId={}", [$clientId]);
    }
    
    /**
     * Enable metrics collection
     * 
     * @param string|null $exportEndpoint OTLP export endpoint (optional)
     * @param int $exportInterval Export interval in seconds
     * @return void
     */
    public function enable(?string $exportEndpoint = null, int $exportInterval = 60): void {
        $this->enabled = true;
        $this->exportEndpoint = $exportEndpoint;
        $this->exportInterval = $exportInterval;
        
        Logger::info("Metrics enabled, clientId={}, exportEndpoint={}, interval={}s", [
            $this->clientId,
            $exportEndpoint ?? 'local-only',
            $exportInterval
        ]);
    }
    
    /**
     * Disable metrics collection
     * 
     * @return void
     */
    public function disable(): void {
        $this->enabled = false;
        Logger::info("Metrics disabled, clientId={}", [$this->clientId]);
    }
    
    /**
     * Check if metrics is enabled
     * 
     * @return bool True if enabled
     */
    public function isEnabled(): bool {
        return $this->enabled;
    }
    
    /**
     * Set gauge observer callback
     * 
     * The gauge observer is called periodically to collect gauge values.
     * Callback signature: function(): array
     * Returns: array of ['name' => string, 'value' => float, 'labels' => array]
     * 
     * @param callable $observer Gauge observer callback
     * @return void
     */
    public function setGaugeObserver(callable $observer): void {
        $this->gaugeObserver = $observer;
        Logger::debug("Gauge observer registered, clientId={}", [$this->clientId]);
    }
    
    /**
     * Register a gauge metric
     * 
     * @param string $name Gauge name
     * @param callable $valueProvider Callback to get current value
     * @param array $labels Labels
     * @return void
     */
    public function registerGauge(string $name, callable $valueProvider, array $labels = []): void {
        $this->gauges[] = [
            'name' => $name,
            'provider' => $valueProvider,
            'labels' => $labels,
        ];
        
        Logger::debug("Gauge registered: name={}, labels={}, clientId={}", [
            $name,
            json_encode($labels),
            $this->clientId
        ]);
    }
    
    /**
     * Record a histogram metric
     * 
     * @param string $histogramName Histogram name (use HistogramEnum constants)
     * @param array $labels Labels
     * @param float $value Value to record
     * @return void
     */
    public function record(string $histogramName, array $labels, float $value): void {
        if (!$this->enabled) {
            return;
        }
        
        // Add client_id label if not present
        if (!isset($labels[MetricLabels::CLIENT_ID])) {
            $labels[MetricLabels::CLIENT_ID] = $this->clientId;
        }
        
        $this->metricsCollector->observeHistogram($histogramName, $labels, $value);
        
        Logger::debug("Recorded histogram metric: name={}, value={}, labels={}, clientId={}", [
            $histogramName,
            $value,
            json_encode($labels),
            $this->clientId
        ]);
    }
    
    /**
     * Set a gauge value
     * 
     * @param string $gaugeName Gauge name (use GaugeEnum constants)
     * @param array $labels Labels
     * @param float $value Value to set
     * @return void
     */
    public function setGauge(string $gaugeName, array $labels, float $value): void {
        if (!$this->enabled) {
            return;
        }
        
        // Add client_id label if not present
        if (!isset($labels[MetricLabels::CLIENT_ID])) {
            $labels[MetricLabels::CLIENT_ID] = $this->clientId;
        }
        
        $this->metricsCollector->setGauge($gaugeName, $labels, $value);
        
        Logger::debug("Set gauge metric: name={}, value={}, labels={}, clientId={}", [
            $gaugeName,
            $value,
            json_encode($labels),
            $this->clientId
        ]);
    }
    
    /**
     * Increment a counter
     * 
     * @param string $counterName Counter name
     * @param array $labels Labels
     * @param float $value Increment value (default: 1)
     * @return void
     */
    public function incrementCounter(string $counterName, array $labels = [], float $value = 1): void {
        if (!$this->enabled) {
            return;
        }
        
        // Add client_id label if not present
        if (!isset($labels[MetricLabels::CLIENT_ID])) {
            $labels[MetricLabels::CLIENT_ID] = $this->clientId;
        }
        
        $this->metricsCollector->incrementCounter($counterName, $labels, $value);
    }
    
    /**
     * Update all registered gauges
     * 
     * This method should be called periodically to update gauge values.
     * 
     * @return void
     */
    public function updateGauges(): void {
        if (!$this->enabled) {
            return;
        }
        
        // Call gauge observer if registered
        if ($this->gaugeObserver !== null) {
            try {
                $observer = $this->gaugeObserver;
                $gaugeValues = $observer();
                
                foreach ($gaugeValues as $gaugeData) {
                    if (isset($gaugeData['name']) && isset($gaugeData['value'])) {
                        $labels = $gaugeData['labels'] ?? [];
                        $this->setGauge($gaugeData['name'], $labels, $gaugeData['value']);
                    }
                }
            } catch (\Exception $e) {
                Logger::error("Failed to update gauges from observer: {}, clientId={}", [
                    $e->getMessage(),
                    $this->clientId
                ]);
            }
        }
        
        // Update registered gauges
        foreach ($this->gauges as $gauge) {
            try {
                $provider = $gauge['provider'];
                $value = $provider();
                $this->setGauge($gauge['name'], $gauge['labels'], $value);
            } catch (\Exception $e) {
                Logger::error("Failed to update gauge {}: {}, clientId={}", [
                    $gauge['name'],
                    $e->getMessage(),
                    $this->clientId
                ]);
            }
        }
    }
    
    /**
     * Export metrics to configured endpoint
     * 
     * @return array Export result
     */
    public function exportMetrics(): array {
        if (!$this->enabled) {
            return ['success' => false, 'message' => 'Metrics is disabled'];
        }
        
        try {
            // Update gauges before export
            $this->updateGauges();
            
            // Get all metrics
            $metrics = $this->metricsCollector->getAllMetrics();
            
            // If export endpoint is configured, send to endpoint
            if ($this->exportEndpoint !== null) {
                $result = $this->sendToEndpoint($this->exportEndpoint, $metrics);
                return $result;
            }
            
            // Otherwise, return metrics data
            return [
                'success' => true,
                'metrics' => $metrics,
                'count' => count($metrics),
                'clientId' => $this->clientId,
            ];
            
        } catch (\Exception $e) {
            Logger::error("Failed to export metrics: {}, clientId={}", [
                $e->getMessage(),
                $this->clientId
            ]);
            
            return [
                'success' => false,
                'message' => $e->getMessage(),
            ];
        }
    }
    
    /**
     * Send metrics to OTLP endpoint
     * 
     * @param string $endpoint OTLP endpoint URL
     * @param array $metrics Metrics data
     * @return array Result
     */
    private function sendToEndpoint(string $endpoint, array $metrics): array {
        // TODO: Implement OTLP export
        // For now, just return the metrics
        Logger::warn("OTLP export not yet implemented, returning metrics locally");
        
        return [
            'success' => true,
            'metrics' => $metrics,
            'count' => count($metrics),
            'endpoint' => $endpoint,
            'note' => 'OTLP export not implemented yet',
        ];
    }
    
    /**
     * Get metrics collector
     * 
     * @return MetricsCollector
     */
    public function getMetricsCollector(): MetricsCollector {
        return $this->metricsCollector;
    }
    
    /**
     * Get client ID
     * 
     * @return string
     */
    public function getClientId(): string {
        return $this->clientId;
    }
    
    /**
     * Get export endpoint
     * 
     * @return string|null
     */
    public function getExportEndpoint(): ?string {
        return $this->exportEndpoint;
    }
    
    /**
     * Shutdown the meter manager
     * 
     * @return void
     */
    public function shutdown(): void {
        $this->disable();
        $this->gauges = [];
        $this->gaugeObserver = null;
        
        Logger::info("ClientMeterManager shutdown, clientId={}", [$this->clientId]);
    }
}
