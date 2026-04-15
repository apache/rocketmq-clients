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
 * Metric Type Enum
 */
class MetricType
{
    const COUNTER = 'COUNTER';           // Counter (monotonically increasing)
    const GAUGE = 'GAUGE';               // Gauge (can increase or decrease)
    const HISTOGRAM = 'HISTOGRAM';       // Histogram (distribution statistics)
    const SUMMARY = 'SUMMARY';           // Summary (P50/P90/P99, etc.)
}

/**
 * Metric Name Enum
 */
class MetricName
{
    // Producer metrics
    const SEND_TOTAL = 'rocketmq_send_total';                          // Total sends
    const SEND_SUCCESS_TOTAL = 'rocketmq_send_success_total';          // Successful sends
    const SEND_FAILURE_TOTAL = 'rocketmq_send_failure_total';          // Failed sends
    const SEND_COST_TIME = 'rocketmq_send_cost_time';                  // Send latency (ms)
    const SEND_QPS = 'rocketmq_send_qps';                              // Send QPS
    
    const BATCH_SEND_TOTAL = 'rocketmq_batch_send_total';              // Total batch sends
    const BATCH_SEND_MESSAGE_TOTAL = 'rocketmq_batch_send_message_total'; // Total messages in batch sends
    
    // Recall metrics
    const RECALL_TOTAL = 'rocketmq_recall_total';                      // Total recalls
    const RECALL_SUCCESS_TOTAL = 'rocketmq_recall_success_total';      // Successful recalls
    const RECALL_FAILURE_TOTAL = 'rocketmq_recall_failure_total';      // Failed recalls
    const RECALL_COST_TIME = 'rocketmq_recall_cost_time';              // Recall latency (ms)
    
    // Consumer metrics
    const RECEIVE_TOTAL = 'rocketmq_receive_total';                    // Total receives
    const CONSUME_TOTAL = 'rocketmq_consume_total';                    // Total consumes
    const CONSUME_SUCCESS_TOTAL = 'rocketmq_consume_success_total';    // Successful consumes
    const CONSUME_FAILURE_TOTAL = 'rocketmq_consume_failure_total';    // Failed consumes
    const CONSUME_COST_TIME = 'rocketmq_consume_cost_time';            // Consume latency (ms)
    const CONSUME_AWAIT_TIME = 'rocketmq_consume_await_time';          // Consume await time (ms)
    
    // Connection metrics
    const CONNECTION_TOTAL = 'rocketmq_connection_total';              // Total connections
    const HEARTBEAT_TOTAL = 'rocketmq_heartbeat_total';                // Total heartbeats
    const HEARTBEAT_FAILURE_TOTAL = 'rocketmq_heartbeat_failure_total'; // Failed heartbeats
    
    // Retry metrics
    const RETRY_TOTAL = 'rocketmq_retry_total';                        // Total retries
    const RETRY_SUCCESS_TOTAL = 'rocketmq_retry_success_total';        // Successful retries
    
    // Connection pool metrics
    const CONNECTION_POOL_SIZE = 'rocketmq_connection_pool_size';      // Current connection pool size
    const CONNECTION_POOL_MAX_SIZE = 'rocketmq_connection_pool_max_size'; // Maximum connection pool size
    const CONNECTION_POOL_BORROW = 'rocketmq_connection_pool_borrow';  // Connection borrow count
    const CONNECTION_POOL_RETURN = 'rocketmq_connection_pool_return';  // Connection return count
    const CONNECTION_POOL_WAIT_TIME = 'rocketmq_connection_pool_wait_time'; // Connection wait time (ms)
    
    // Cache metrics
    const CACHE_HITS = 'rocketmq_cache_hits';                          // Cache hits
    const CACHE_MISSES = 'rocketmq_cache_misses';                      // Cache misses
    const CACHE_REFRESHES = 'rocketmq_cache_refreshes';                // Cache refreshes
    const CACHE_EVICTIONS = 'rocketmq_cache_evictions';                // Cache evictions
    const CACHE_SIZE = 'rocketmq_cache_size';                          // Current cache size
    const CACHE_MAX_SIZE = 'rocketmq_cache_max_size';                  // Maximum cache size
    
    // Client state metrics
    const CLIENT_STATE = 'rocketmq_client_state';                      // Client state (0: CREATED, 1: RUNNING, 2: STOPPING, 3: TERMINATED)
    const CLIENT_UPTIME = 'rocketmq_client_uptime';                    // Client uptime (seconds)
    
    // System metrics
    const MEMORY_USAGE = 'rocketmq_memory_usage';                      // Memory usage (bytes)
    const CPU_USAGE = 'rocketmq_cpu_usage';                            // CPU usage (percent)
}

/**
 * Metric Labels - Standard labels for metrics
 * 
 * Reference: Java MetricLabels
 */
class MetricLabels
{
    /**
     * Topic label key
     */
    const TOPIC = 'topic';
    
    /**
     * Client ID label key
     */
    const CLIENT_ID = 'client_id';
    
    /**
     * Consumer group label key
     */
    const CONSUMER_GROUP = 'consumer_group';
    
    /**
     * Invocation status label key
     */
    const INVOCATION_STATUS = 'invocation_status';
    
    /**
     * Status label key (legacy)
     * @deprecated Use INVOCATION_STATUS instead
     */
    const STATUS = 'status';
    
    /**
     * Endpoints label key
     */
    const ENDPOINTS = 'endpoints';
    
    /**
     * Message type label key
     */
    const MESSAGE_TYPE = 'message_type';
    
    /**
     * Get all standard label keys
     * 
     * @return array Array of label keys
     */
    public static function getAll(): array {
        return [
            self::TOPIC,
            self::CLIENT_ID,
            self::CONSUMER_GROUP,
            self::INVOCATION_STATUS,
        ];
    }
}

/**
 * Metric Data Point
 */
class MetricDataPoint
{
    public $name;              // Metric name
    public $type;              // Metric type
    public $value;             // Value
    public $labels = [];       // Labels
    public $timestamp;         // Timestamp
    public $count = 0;         // Count (for Histogram/Summary)
    public $sum = 0;           // Sum (for Histogram/Summary)
    public $buckets = [];      // Buckets (for Histogram)
    public $quantiles = [];    // Quantiles (for Summary)
    
    public function __construct($name, $type, $value = 0)
    {
        $this->name = $name;
        $this->type = $type;
        $this->value = $value;
        $this->timestamp = microtime(true);
    }
    
    /**
     * Add label
     */
    public function addLabel($key, $value)
    {
        $this->labels[$key] = $value;
        return $this;
    }
    
    /**
     * Convert to array
     */
    public function toArray()
    {
        $data = [
            'name' => $this->name,
            'type' => $this->type,
            'value' => $this->value,
            'labels' => $this->labels,
            'timestamp' => $this->timestamp,
        ];
        
        if ($this->count > 0) {
            $data['count'] = $this->count;
        }
        
        if ($this->sum > 0) {
            $data['sum'] = $this->sum;
        }
        
        if (!empty($this->buckets)) {
            $data['buckets'] = $this->buckets;
        }
        
        if (!empty($this->quantiles)) {
            $data['quantiles'] = $this->quantiles;
        }
        
        return $data;
    }
}

/**
 * Metrics Collector
 * 
 * Responsible for collecting and storing various performance metrics, supporting four types: Counter, Gauge, Histogram, and Summary
 */
class MetricsCollector
{
    /**
     * @var string Client ID
     */
    private $clientId;
    
    /**
     * @var array Metrics data storage
     */
    private $metrics = [];
    
    /**
     * @var array Histogram bucket configuration
     */
    private $histogramBuckets = [
        MetricName::SEND_COST_TIME => [1, 5, 10, 20, 50, 100, 200, 500, 1000],
        MetricName::CONSUME_COST_TIME => [1, 5, 10, 20, 50, 100, 200, 500, 1000, 5000],
        MetricName::CONSUME_AWAIT_TIME => [1, 5, 20, 100, 1000, 5000, 10000],
    ];
    
    /**
     * @var int Maximum number of historical data points to retain
     */
    private $maxHistoryPoints = 1000;
    
    /**
     * @var array Historical data points (for calculating rates and trends)
     */
    private $historyPoints = [];
    
    /**
     * Constructor
     * 
     * @param string $clientId Client ID
     */
    public function __construct($clientId)
    {
        $this->clientId = $clientId;
    }
    
    /**
     * Increment counter
     * 
     * @param string $name Metric name
     * @param array $labels Labels
     * @param float $value Increment value (default 1)
     * @return void
     */
    public function incrementCounter($name, $labels = [], $value = 1)
    {
        $key = $this->buildMetricKey($name, $labels);
        
        if (!isset($this->metrics[$key])) {
            $this->metrics[$key] = new MetricDataPoint($name, MetricType::COUNTER, 0);
            foreach ($labels as $k => $v) {
                $this->metrics[$key]->addLabel($k, $v);
            }
        }
        
        $this->metrics[$key]->value += $value;
        $this->metrics[$key]->timestamp = microtime(true);
        
        $this->recordHistory($key, $this->metrics[$key]);
    }
    
    /**
     * Set gauge value
     * 
     * @param string $name Metric name
     * @param array $labels Labels
     * @param float $value Value
     * @return void
     */
    public function setGauge($name, $labels = [], $value = 0)
    {
        $key = $this->buildMetricKey($name, $labels);
        
        $point = new MetricDataPoint($name, MetricType::GAUGE, $value);
        foreach ($labels as $k => $v) {
            $point->addLabel($k, $v);
        }
        
        $this->metrics[$key] = $point;
        $this->recordHistory($key, $point);
    }
    
    /**
     * Observe histogram
     * 
     * @param string $name Metric name
     * @param array $labels Labels
     * @param float $value Observed value
     * @return void
     */
    public function observeHistogram($name, $labels = [], $value = 0)
    {
        $key = $this->buildMetricKey($name, $labels);
        
        if (!isset($this->metrics[$key])) {
            $this->metrics[$key] = new MetricDataPoint($name, MetricType::HISTOGRAM, 0);
            foreach ($labels as $k => $v) {
                $this->metrics[$key]->addLabel($k, $v);
            }
            
            // Initialize buckets
            $buckets = isset($this->histogramBuckets[$name]) ? $this->histogramBuckets[$name] : [1, 5, 10, 50, 100, 500, 1000];
            foreach ($buckets as $boundary) {
                $this->metrics[$key]->buckets[$boundary] = 0;
            }
            $this->metrics[$key]->buckets['+Inf'] = 0;
        }
        
        $point = $this->metrics[$key];
        $point->count++;
        $point->sum += $value;
        
        // Update bucket counts
        foreach ($point->buckets as $boundary => $count) {
            if ($boundary === '+Inf' || $value <= $boundary) {
                $point->buckets[$boundary]++;
            }
        }
        
        $point->timestamp = microtime(true);
        $this->recordHistory($key, $point);
    }
    
    /**
     * Record summary
     * 
     * @param string $name Metric name
     * @param array $labels Labels
     * @param float $value Observed value
     * @return void
     */
    public function recordSummary($name, $labels = [], $value = 0)
    {
        $key = $this->buildMetricKey($name, $labels);
        
        if (!isset($this->metrics[$key])) {
            $this->metrics[$key] = new MetricDataPoint($name, MetricType::SUMMARY, 0);
            foreach ($labels as $k => $v) {
                $this->metrics[$key]->addLabel($k, $v);
            }
            $this->metrics[$key]->quantiles = [0.5 => 0, 0.9 => 0, 0.95 => 0, 0.99 => 0];
        }
        
        $point = $this->metrics[$key];
        $point->count++;
        $point->sum += $value;
        
        // Simplified quantile calculation (actual implementation should use more precise algorithms)
        if ($point->count > 0) {
            $point->value = $point->sum / $point->count; // Average
            $point->quantiles[0.5] = $point->value * 0.8;  // P50 estimation
            $point->quantiles[0.9] = $point->value * 1.2;  // P90 estimation
            $point->quantiles[0.95] = $point->value * 1.5; // P95 estimation
            $point->quantiles[0.99] = $point->value * 2.0; // P99 estimation
        }
        
        $point->timestamp = microtime(true);
        $this->recordHistory($key, $point);
    }
    
    /**
     * Get all metrics
     * 
     * @return array
     */
    public function getAllMetrics()
    {
        $result = [];
        foreach ($this->metrics as $key => $point) {
            $result[] = $point->toArray();
        }
        return $result;
    }
    
    /**
     * Get specific metric
     * 
     * @param string $name Metric name
     * @param array $labels Label filter
     * @return array|null
     */
    public function getMetric($name, $labels = [])
    {
        $key = $this->buildMetricKey($name, $labels);
        return isset($this->metrics[$key]) ? $this->metrics[$key]->toArray() : null;
    }
    
    /**
     * Export to Prometheus format
     * 
     * @return string
     */
    public function exportToPrometheus()
    {
        $output = [];
        
        foreach ($this->metrics as $key => $point) {
            $labelsStr = '';
            if (!empty($point->labels)) {
                $labelPairs = [];
                foreach ($point->labels as $k => $v) {
                    $labelPairs[] = "{$k}=\"{$v}\"";
                }
                $labelsStr = '{' . implode(',', $labelPairs) . '}';
            }
            
            switch ($point->type) {
                case MetricType::COUNTER:
                case MetricType::GAUGE:
                    $output[] = "# HELP {$point->name} {$point->name} metric";
                    $output[] = "# TYPE {$point->name} " . strtolower($point->type);
                    $output[] = "{$point->name}{$labelsStr} {$point->value}";
                    break;
                    
                case MetricType::HISTOGRAM:
                    $output[] = "# HELP {$point->name} {$point->name} metric";
                    $output[] = "# TYPE {$point->name} histogram";
                    
                    foreach ($point->buckets as $boundary => $count) {
                        $le = $boundary === '+Inf' ? '+Inf' : $boundary;
                        $output[] = "{$point->name}_bucket{$labelsStr},le={$le} {$count}";
                    }
                    $output[] = "{$point->name}_sum{$labelsStr} {$point->sum}";
                    $output[] = "{$point->name}_count{$labelsStr} {$point->count}";
                    break;
                    
                case MetricType::SUMMARY:
                    $output[] = "# HELP {$point->name} {$point->name} metric";
                    $output[] = "# TYPE {$point->name} summary";
                    
                    foreach ($point->quantiles as $quantile => $value) {
                        $output[] = "{$point->name}{$labelsStr},quantile={$quantile} {$value}";
                    }
                    $output[] = "{$point->name}_sum{$labelsStr} {$point->sum}";
                    $output[] = "{$point->name}_count{$labelsStr} {$point->count}";
                    break;
            }
        }
        
        return implode("\n", $output) . "\n";
    }
    
    /**
     * Export to JSON format
     * 
     * @return string
     */
    public function exportToJson()
    {
        $data = [
            'clientId' => $this->clientId,
            'timestamp' => date('Y-m-d H:i:s'),
            'metrics' => $this->getAllMetrics(),
        ];
        
        return json_encode($data, JSON_PRETTY_PRINT | JSON_UNESCAPED_UNICODE);
    }
    
    /**
     * Clear all metrics
     * 
     * @return void
     */
    public function clear()
    {
        $this->metrics = [];
        $this->historyPoints = [];
    }
    
    /**
     * Get client ID
     * 
     * @return string
     */
    public function getClientId()
    {
        return $this->clientId;
    }
    
    /**
     * Build metric key
     * 
     * @param string $name Metric name
     * @param array $labels Labels
     * @return string
     */
    private function buildMetricKey($name, $labels)
    {
        ksort($labels);
        return $name . '|' . md5(json_encode($labels));
    }
    
    /**
     * Record historical data point
     * 
     * @param string $key Metric key
     * @param MetricDataPoint $point Data point
     * @return void
     */
    private function recordHistory($key, $point)
    {
        if (!isset($this->historyPoints[$key])) {
            $this->historyPoints[$key] = [];
        }
        
        $this->historyPoints[$key][] = [
            'value' => $point->value,
            'timestamp' => $point->timestamp,
        ];
        
        // Limit history count
        if (count($this->historyPoints[$key]) > $this->maxHistoryPoints) {
            array_shift($this->historyPoints[$key]);
        }
    }
    
    /**
     * Calculate metric growth rate
     * 
     * @param string $name Metric name
     * @param array $labels Labels
     * @return float|null Growth rate (per second)
     */
    public function calculateRate($name, $labels = [])
    {
        $key = $this->buildMetricKey($name, $labels);
        
        if (!isset($this->historyPoints[$key]) || count($this->historyPoints[$key]) < 2) {
            return null;
        }
        
        $history = $this->historyPoints[$key];
        $last = end($history);
        $first = reset($history);
        
        $timeDiff = $last['timestamp'] - $first['timestamp'];
        if ($timeDiff <= 0) {
            return null;
        }
        
        $valueDiff = $last['value'] - $first['value'];
        return $valueDiff / $timeDiff; // Growth rate per second
    }
}
