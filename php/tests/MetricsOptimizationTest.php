<?php
/**
 * Metrics Performance Optimization Guide
 * 
 * This file contains examples and best practices for optimizing
 * metrics collection performance in production environments.
 */

// ========================================
// 1. BATCH EXPORT PATTERN
// ========================================

/**
 * Batch export collects metrics and exports them in batches
 * to reduce overhead and improve throughput.
 * 
 * Implementation strategy:
 * - Collect metrics locally
 * - Export periodically (e.g., every 60 seconds)
 * - Use background thread/process for export
 */
class BatchMetricsExporter {
    private $meterManager;
    private $exportInterval = 60; // seconds
    private $lastExportTime = 0;
    
    public function __construct($meterManager, int $exportInterval = 60) {
        $this->meterManager = $meterManager;
        $this->exportInterval = $exportInterval;
    }
    
    /**
     * Check if it's time to export
     */
    public function shouldExport(): bool {
        $now = time();
        return ($now - $this->lastExportTime) >= $this->exportInterval;
    }
    
    /**
     * Export metrics if needed
     */
    public function exportIfNeeded(): array {
        if ($this->shouldExport()) {
            $this->lastExportTime = time();
            return $this->meterManager->exportMetrics();
        }
        return ['success' => false, 'message' => 'Not time to export yet'];
    }
}

// Usage:
// $exporter = new BatchMetricsExporter($meterManager, 60);
// In your main loop:
// if ($exporter->shouldExport()) {
//     $result = $exporter->exportIfNeeded();
// }


// ========================================
// 2. ASYNC REPORTING PATTERN
// ========================================

/**
 * Async reporting uses background processes/threads to export metrics
 * without blocking the main application flow.
 * 
 * For PHP, you can use:
 * - pcntl_fork() for CLI applications
 * - Swoole async tasks
 * - ReactPHP event loop
 * - Separate worker process
 */
class AsyncMetricsReporter {
    private $meterManager;
    private $pidFile = '/tmp/metrics_reporter.pid';
    
    public function __construct($meterManager) {
        $this->meterManager = $meterManager;
    }
    
    /**
     * Start async reporter (CLI only)
     */
    public function startAsyncReporter(int $interval = 60): void {
        if (!extension_loaded('pcntl')) {
            throw new \RuntimeException("pcntl extension required for async reporting");
        }
        
        $pid = pcntl_fork();
        
        if ($pid == -1) {
            throw new \RuntimeException("Failed to fork process");
        } elseif ($pid > 0) {
            // Parent process
            file_put_contents($this->pidFile, $pid);
            echo "Metrics reporter started with PID: $pid\n";
        } else {
            // Child process - reporter
            $this->runReporter($interval);
            exit(0);
        }
    }
    
    /**
     * Run the reporter loop
     */
    private function runReporter(int $interval): void {
        while (true) {
            sleep($interval);
            
            try {
                $result = $this->meterManager->exportMetrics();
                
                if (!$result['success']) {
                    error_log("Metrics export failed: " . ($result['message'] ?? 'Unknown'));
                }
            } catch (\Exception $e) {
                error_log("Metrics reporter error: " . $e->getMessage());
            }
        }
    }
    
    /**
     * Stop async reporter
     */
    public function stopAsyncReporter(): void {
        if (file_exists($this->pidFile)) {
            $pid = intval(file_get_contents($this->pidFile));
            posix_kill($pid, SIGTERM);
            unlink($this->pidFile);
        }
    }
}


// ========================================
// 3. METRICS SAMPLING
// ========================================

/**
 * For high-throughput scenarios, sample metrics to reduce overhead.
 * Only record a percentage of operations.
 */
class SamplingMetricsCollector {
    private $meterManager;
    private $sampleRate; // 0.0 to 1.0
    
    public function __construct($meterManager, float $sampleRate = 0.1) {
        $this->meterManager = $meterManager;
        $this->sampleRate = max(0.0, min(1.0, $sampleRate));
    }
    
    /**
     * Record metric with sampling
     */
    public function recordWithSampling(string $histogramName, array $labels, float $value): void {
        if (mt_rand() / mt_getrandmax() <= $this->sampleRate) {
            $this->meterManager->record($histogramName, $labels, $value);
        }
    }
}

// Usage:
// $sampler = new SamplingMetricsCollector($meterManager, 0.1); // 10% sampling
// $sampler->recordWithSampling(HistogramEnum::SEND_COST_TIME, $labels, $duration);


// ========================================
// 4. LABEL CARDINALITY CONTROL
// ========================================

/**
 * High cardinality labels (many unique values) can cause memory issues.
 * Best practices:
 * - Avoid using message IDs, timestamps, or UUIDs as labels
 * - Use bounded sets for label values
 * - Aggregate when possible
 */
class LabelCardinalityController {
    private $maxUniqueLabels = 1000;
    private $labelCache = [];
    
    /**
     * Check if adding this label combination is safe
     */
    public function isLabelSafe(array $labels): bool {
        $key = md5(json_encode($labels));
        
        if (isset($this->labelCache[$key])) {
            return true; // Already seen
        }
        
        if (count($this->labelCache) >= $this->maxUniqueLabels) {
            return false; // Too many unique labels
        }
        
        $this->labelCache[$key] = true;
        return true;
    }
    
    /**
     * Clear old labels periodically
     */
    public function clearOldLabels(): void {
        // Keep only recent labels
        $this->labelCache = array_slice($this->labelCache, -500, null, true);
    }
}


// ========================================
// 5. MEMORY MANAGEMENT
// ========================================

/**
 * Monitor and control memory usage of metrics collector
 */
class MetricsMemoryManager {
    private $metricsCollector;
    private $maxMemoryMB = 100;
    
    public function __construct($metricsCollector, int $maxMemoryMB = 100) {
        $this->metricsCollector = $metricsCollector;
        $this->maxMemoryMB = $maxMemoryMB;
    }
    
    /**
     * Check current memory usage
     */
    public function getMemoryUsageMB(): float {
        return memory_get_usage(true) / 1024 / 1024;
    }
    
    /**
     * Check if memory usage is within limits
     */
    public function isMemoryOK(): bool {
        return $this->getMemoryUsageMB() < $this->maxMemoryMB;
    }
    
    /**
     * Cleanup if memory is too high
     */
    public function cleanupIfNeeded(): void {
        if (!$this->isMemoryOK()) {
            // Export and clear metrics
            $this->metricsCollector->exportMetrics();
            
            // Force garbage collection
            gc_collect_cycles();
        }
    }
}


// ========================================
// 6. PROMETHEUS INTEGRATION EXAMPLE
// ========================================

/**
 * Export metrics in Prometheus format
 * Note: This is a simplified example. For production, use
 * the official prometheus/client_php library.
 */
class PrometheusExporter {
    private $meterManager;
    
    public function __construct($meterManager) {
        $this->meterManager = $meterManager;
    }
    
    /**
     * Export metrics in Prometheus text format
     */
    public function exportToPrometheusFormat(): string {
        $result = $this->meterManager->exportMetrics();
        
        if (!$result['success']) {
            return "# Error exporting metrics\n";
        }
        
        $output = [];
        
        foreach ($result['metrics'] as $metric) {
            $name = $metric['name'];
            $value = $metric['value'];
            $labels = $metric['labels'] ?? [];
            
            // Format labels
            $labelParts = [];
            foreach ($labels as $key => $val) {
                $labelParts[] = "{$key}=\"{$val}\"";
            }
            $labelStr = !empty($labelParts) ? '{' . implode(',', $labelParts) . '}' : '';
            
            $output[] = "{$name}{$labelStr} {$value}";
        }
        
        return implode("\n", $output) . "\n";
    }
    
    /**
     * Serve metrics on HTTP endpoint (for scraping by Prometheus)
     */
    public function serveMetricsEndpoint(string $host = '0.0.0.0', int $port = 9090): void {
        $socket = socket_create(AF_INET, SOCK_STREAM, SOL_TCP);
        socket_set_option($socket, SOL_SOCKET, SO_REUSEADDR, 1);
        socket_bind($socket, $host, $port);
        socket_listen($socket);
        
        echo "Serving metrics on http://{$host}:{$port}/metrics\n";
        
        while (true) {
            $client = socket_accept($socket);
            
            $request = socket_read($client, 1024);
            
            // Simple HTTP response
            $metrics = $this->exportToPrometheusFormat();
            $response = "HTTP/1.1 200 OK\r\n" .
                       "Content-Type: text/plain\r\n" .
                       "Content-Length: " . strlen($metrics) . "\r\n" .
                       "\r\n" .
                       $metrics;
            
            socket_write($client, $response);
            socket_close($client);
        }
        
        socket_close($socket);
    }
}


// ========================================
// 7. GRAFANA DASHBOARD TIPS
// ========================================

/*
 * Recommended Grafana queries:
 * 
 * 1. Message send rate:
 *    rate(rocketmq_send_cost_time_count[1m])
 * 
 * 2. Average send latency:
 *    rate(rocketmq_send_cost_time_sum[1m]) / rate(rocketmq_send_cost_time_count[1m])
 * 
 * 3. Send latency percentiles:
 *    histogram_quantile(0.95, rate(rocketmq_send_cost_time_bucket[5m]))
 * 
 * 4. Consumer lag (if available):
 *    rocketmq_consumer_cached_messages
 * 
 * 5. Error rate:
 *    rate(rocketmq_send_cost_time_count{invocation_status="failure"}[1m])
 */


echo "This file contains examples and patterns. See comments for usage.\n";
