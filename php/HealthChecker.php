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

use Apache\Rocketmq\V2\ClientType;
use Apache\Rocketmq\V2\HeartbeatRequest;
use Apache\Rocketmq\V2\MessagingServiceClient;
use Apache\Rocketmq\V2\Resource;
use Grpc\ChannelCredentials;
use const Grpc\STATUS_OK;

/**
 * Connection Health Status Enum
 */
class HealthStatus
{
    /**
     * Healthy - Connection is normal
     */
    const HEALTHY = 'HEALTHY';
    
    /**
     * Unhealthy - Connection is abnormal
     */
    const UNHEALTHY = 'UNHEALTHY';
    
    /**
     * Unknown - Not checked yet
     */
    const UNKNOWN = 'UNKNOWN';
    
    /**
     * Degraded - Partial functionality unavailable
     */
    const DEGRADED = 'DEGRADED';
}

/**
 * Health Check Result
 */
class HealthCheckResult
{
    /**
     * @var string Health status
     */
    public $status;
    
    /**
     * @var int Last check timestamp
     */
    public $lastCheckTime;
    
    /**
     * @var int Consecutive failure count
     */
    public $consecutiveFailures;
    
    /**
     * @var string|null Error message
     */
    public $errorMessage;
    
    /**
     * @var float Response time (milliseconds)
     */
    public $responseTime;
    
    public function __construct($status = HealthStatus::UNKNOWN)
    {
        $this->status = $status;
        $this->lastCheckTime = time();
        $this->consecutiveFailures = 0;
        $this->errorMessage = null;
        $this->responseTime = 0;
    }
    
    /**
     * Is healthy
     */
    public function isHealthy()
    {
        return $this->status === HealthStatus::HEALTHY;
    }
    
    /**
     * Convert to array
     */
    public function toArray()
    {
        return [
            'status' => $this->status,
            'lastCheckTime' => $this->lastCheckTime,
            'consecutiveFailures' => $this->consecutiveFailures,
            'errorMessage' => $this->errorMessage,
            'responseTime' => $this->responseTime,
            'isHealthy' => $this->isHealthy(),
        ];
    }
}

/**
 * Health Checker
 * 
 * Responsible for monitoring the connection health status between Producer/Consumer and RocketMQ server
 * Detects whether the connection is normal by periodically sending heartbeats
 * 
 * Usage example:
 * $checker = new HealthChecker($endpoints, $clientId);
 * $result = $checker->check();
 * if (!$result->isHealthy()) {
 *     // Handle connection exception
 * }
 */
class HealthChecker
{
    /**
     * @var string Server endpoints
     */
    private $endpoints;
    
    /**
     * @var string Client ID
     */
    private $clientId;
    
    /**
     * @var ClientType Client type
     */
    private $clientType;
    
    /**
     * @var MessagingServiceClient gRPC client
     */
    private $grpcClient;
    
    /**
     * @var HealthCheckResult Latest check result
     */
    private $lastResult;
    
    /**
     * @var int Maximum consecutive failures threshold
     */
    private $maxConsecutiveFailures;
    
    /**
     * @var int Heartbeat timeout (seconds)
     */
    private $timeoutSeconds;
    
    /**
     * @var array Historical check results (for statistics)
     */
    private $historyResults = [];
    
    /**
     * @var int Maximum history records
     */
    private $maxHistorySize = 100;
    
    /**
     * Constructor
     * 
     * @param string $endpoints Server endpoints
     * @param string $clientId Client ID
     * @param int $clientType Client type (SIMPLE_CONSUMER/PUSH_CONSUMER/PRODUCER)
     * @param int $maxConsecutiveFailures Maximum consecutive failures
     * @param int $timeoutSeconds Timeout (seconds)
     */
    public function __construct(
        $endpoints,
        $clientId,
        $clientType = ClientType::PRODUCER,
        $maxConsecutiveFailures = 3,
        $timeoutSeconds = 5
    ) {
        $this->endpoints = $endpoints;
        $this->clientId = $clientId;
        $this->clientType = $clientType;
        $this->maxConsecutiveFailures = $maxConsecutiveFailures;
        $this->timeoutSeconds = $timeoutSeconds;
        $this->lastResult = new HealthCheckResult(HealthStatus::UNKNOWN);
        
        // Initialize gRPC client
        $this->grpcClient = new MessagingServiceClient($endpoints, [
            'credentials' => ChannelCredentials::createInsecure(),
            'update_metadata' => function ($metaData) {
                $metaData['headers'] = ['clientID' => $this->clientId];
                return $metaData;
            }
        ]);
    }
    
    /**
     * Execute health check
     * 
     * @return HealthCheckResult Check result
     */
    public function check()
    {
        $startTime = microtime(true);
        
        try {
            // Send heartbeat request
            $request = $this->buildHeartbeatRequest();
            list($response, $status) = $this->grpcClient->Heartbeat($request)->wait();
            
            $endTime = microtime(true);
            $responseTime = ($endTime - $startTime) * 1000; // Convert to milliseconds
            
            if ($status->code !== STATUS_OK) {
                // gRPC call failed
                return $this->handleFailure("gRPC call failed: " . $status->details, $responseTime);
            }
            
            // Check response status
            if ($response->getStatus()->getCode() !== 0) {
                return $this->handleFailure(
                    "Heartbeat failed: " . $response->getStatus()->getMessage(),
                    $responseTime
                );
            }
            
            // Check succeeded
            return $this->handleSuccess($responseTime);
            
        } catch (\Exception $e) {
            $endTime = microtime(true);
            $responseTime = ($endTime - $startTime) * 1000;
            return $this->handleFailure("Exception: " . $e->getMessage(), $responseTime);
        }
    }
    
    /**
     * Build heartbeat request
     * 
     * @return HeartbeatRequest
     */
    private function buildHeartbeatRequest()
    {
        $request = new HeartbeatRequest();
        
        $settings = new Settings();
        $settings->setClientType($this->clientType);
        
        $request->setSettings($settings);
        
        return $request;
    }
    
    /**
     * Handle success result
     * 
     * @param float $responseTime Response time
     * @return HealthCheckResult
     */
    private function handleSuccess($responseTime)
    {
        $result = new HealthCheckResult(HealthStatus::HEALTHY);
        $result->responseTime = $responseTime;
        $result->consecutiveFailures = 0;
        
        // If there was an unhealthy record before, log recovery information
        if (!$this->lastResult->isHealthy()) {
            \Apache\Rocketmq\Logger::info(
                "Connection recovered after {} failures. Response time: {}ms, clientId={}",
                [$this->lastResult->consecutiveFailures, round($responseTime, 2), $this->clientId]
            );
        }
        
        $this->updateLastResult($result);
        return $result;
    }
    
    /**
     * Handle failure result
     * 
     * @param string $errorMessage Error message
     * @param float $responseTime Response time
     * @return HealthCheckResult
     */
    private function handleFailure($errorMessage, $responseTime)
    {
        $consecutiveFailures = $this->lastResult->consecutiveFailures + 1;
        
        // Determine health status
        $status = HealthStatus::UNHEALTHY;
        if ($consecutiveFailures >= $this->maxConsecutiveFailures) {
            $status = HealthStatus::UNHEALTHY;
        } elseif ($responseTime > 1000) {
            // Response time exceeds 1 second, mark as degraded
            $status = HealthStatus::DEGRADED;
        }
        
        $result = new HealthCheckResult($status);
        $result->errorMessage = $errorMessage;
        $result->consecutiveFailures = $consecutiveFailures;
        $result->responseTime = $responseTime;
        
        // Log warning
        if ($consecutiveFailures >= $this->maxConsecutiveFailures) {
            \Apache\Rocketmq\Logger::error(
                "Connection unhealthy after {} consecutive failures. Error: {}, clientId={}",
                [$consecutiveFailures, $errorMessage, $this->clientId]
            );
        }
        
        $this->updateLastResult($result);
        return $result;
    }
    
    /**
     * Update last check result
     * 
     * @param HealthCheckResult $result
     */
    private function updateLastResult($result)
    {
        $this->lastResult = $result;
        
        // Add to history
        $this->historyResults[] = $result;
        
        // Limit history size
        if (count($this->historyResults) > $this->maxHistorySize) {
            array_shift($this->historyResults);
        }
    }
    
    /**
     * Get last check result
     * 
     * @return HealthCheckResult
     */
    public function getLastResult()
    {
        return $this->lastResult;
    }
    
    /**
     * Is healthy
     * 
     * @return bool
     */
    public function isHealthy()
    {
        return $this->lastResult->isHealthy();
    }
    
    /**
     * Get health statistics
     * 
     * @return array
     */
    public function getStats()
    {
        $totalChecks = count($this->historyResults);
        $healthyChecks = 0;
        $totalResponseTime = 0;
        $maxResponseTime = 0;
        $minResponseTime = PHP_FLOAT_MAX;
        
        foreach ($this->historyResults as $result) {
            if ($result->isHealthy()) {
                $healthyChecks++;
            }
            $totalResponseTime += $result->responseTime;
            $maxResponseTime = max($maxResponseTime, $result->responseTime);
            $minResponseTime = min($minResponseTime, $result->responseTime);
        }
        
        $avgResponseTime = $totalChecks > 0 ? $totalResponseTime / $totalChecks : 0;
        $healthRate = $totalChecks > 0 ? ($healthyChecks / $totalChecks) * 100 : 0;
        
        return [
            'totalChecks' => $totalChecks,
            'healthyChecks' => $healthyChecks,
            'healthRate' => round($healthRate, 2),
            'avgResponseTime' => round($avgResponseTime, 2),
            'maxResponseTime' => round($maxResponseTime, 2),
            'minResponseTime' => $minResponseTime === PHP_FLOAT_MAX ? 0 : round($minResponseTime, 2),
            'currentStatus' => $this->lastResult->status,
            'consecutiveFailures' => $this->lastResult->consecutiveFailures,
            'lastCheckTime' => $this->lastResult->lastCheckTime,
        ];
    }
    
    /**
     * Reset health check state
     */
    public function reset()
    {
        $this->lastResult = new HealthCheckResult(HealthStatus::UNKNOWN);
        $this->historyResults = [];
    }
    
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
     * Get client ID
     * 
     * @return string
     */
    public function getClientId()
    {
        return $this->clientId;
    }
}
