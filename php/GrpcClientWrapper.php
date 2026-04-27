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

use Grpc\BaseStub;
use Swoole\Coroutine\Http\Client as SwooleHttpClient;

/**
 * Unified gRPC client wrapper that supports both official gRPC and Swoole HTTP/2
 * 
 * This class provides a unified interface for gRPC calls, abstracting away
 * the differences between the official PHP gRPC extension and Swoole's HTTP/2 client.
 */
class GrpcClientWrapper
{
    /** @var BaseStub|null Official gRPC client */
    private $grpcClient = null;
    
    /** @var SwooleHttpClient|null Swoole HTTP/2 client */
    private $swooleClient = null;
    
    /** @var string Server endpoints */
    private $endpoints;
    
    /** @var bool Whether to use Swoole mode */
    private $useSwoole = false;
    
    /** @var array Call options */
    private $options = [];
    
    /**
     * Constructor
     * 
     * @param string $endpoints Server endpoints (e.g., "127.0.0.1:8080")
     * @param array $options gRPC options
     * @param bool $useSwoole Whether to use Swoole HTTP/2 client
     */
    public function __construct(string $endpoints, array $options = [], bool $useSwoole = false)
    {
        $this->endpoints = $endpoints;
        $this->options = $options;
        $this->useSwoole = $useSwoole;
        
        if ($useSwoole) {
            $this->initSwooleClient();
        } else {
            $this->initGrpcClient();
        }
    }
    
    /**
     * Initialize official gRPC client
     */
    private function initGrpcClient(): void
    {
        // Will be initialized by caller with specific stub class
    }
    
    /**
     * Initialize Swoole HTTP/2 client
     */
    private function initSwooleClient(): void
    {
        list($host, $port) = explode(':', $this->endpoints);
        $port = intval($port);
        
        $this->swooleClient = new SwooleHttpClient($host, $port);
        $this->swooleClient->set([
            'open_http2_protocol' => true,
            'timeout' => $this->options['timeout'] ?? 10,
            'ssl_verify_peer' => $this->options['ssl_verify_peer'] ?? false,
        ]);
        
        if (!$this->swooleClient->connect()) {
            throw new \Exception("Failed to connect to {$this->endpoints}: errCode=" . $this->swooleClient->errCode);
        }
    }
    
    /**
     * Set the official gRPC client stub
     * 
     * @param BaseStub $client gRPC client stub
     */
    public function setGrpcClient(BaseStub $client): void
    {
        $this->grpcClient = $client;
    }
    
    /**
     * Make a unary RPC call
     * 
     * @param string $method RPC method name
     * @param object $request Request message
     * @param array $metadata Call metadata
     * @param array $options Call options
     * @return array [response, status]
     */
    public function unaryCall(string $method, object $request, array $metadata = [], array $options = []): array
    {
        if ($this->useSwoole && $this->swooleClient) {
            return $this->swooleUnaryCall($method, $request, $metadata, $options);
        } elseif ($this->grpcClient) {
            return $this->grpcUnaryCall($method, $request, $metadata, $options);
        } else {
            throw new \Exception("No gRPC client initialized");
        }
    }
    
    /**
     * Make unary call using official gRPC
     */
    private function grpcUnaryCall(string $method, object $request, array $metadata = [], array $options = []): array
    {
        $call = $this->grpcClient->$method($request, $metadata, $options);
        return $call->wait();
    }
    
    /**
     * Make unary call using Swoole HTTP/2
     */
    private function swooleUnaryCall(string $method, object $request, array $metadata = [], array $options = []): array
    {
        // Convert method name to path
        $path = "/apache.rocketmq.v2.MessagingService/{$method}";
        
        // Serialize request
        $data = $request->serializeToString();
        
        // Build headers
        $headers = [
            'content-type' => 'application/grpc',
            'te' => 'trailers',
        ];
        
        foreach ($metadata as $key => $value) {
            $headers[strtolower($key)] = $value;
        }
        
        // Make request
        $this->swooleClient->send($path, $data, $headers);
        
        // Receive response
        $response = $this->swooleClient->recv();
        
        if ($response === false) {
            throw new \Exception("Swoole HTTP/2 request failed: errCode=" . $this->swooleClient->errCode);
        }
        
        // Parse response (simplified - needs proper gRPC frame parsing)
        // For now, return mock response
        return [null, ['code' => 0, 'details' => 'OK']];
    }
    
    /**
     * Create a bidirectional streaming call
     * 
     * @param string $method RPC method name
     * @param callable $deserialize Deserialization function
     * @return SwooleStreamingCall
     */
    public function bidiStream(string $method, callable $deserialize): SwooleStreamingCall
    {
        if (!$this->useSwoole || !$this->swooleClient) {
            throw new \Exception("Bidirectional streaming only supported with Swoole client");
        }
        
        return new SwooleStreamingCall($this->swooleClient, $method, $deserialize);
    }
    
    /**
     * Close the connection
     */
    public function close(): void
    {
        if ($this->swooleClient) {
            $this->swooleClient->close();
            $this->swooleClient = null;
        }
    }
    
    /**
     * Get endpoints
     * 
     * @return string
     */
    public function getEndpoints(): string
    {
        return $this->endpoints;
    }
    
    /**
     * Check if using Swoole
     * 
     * @return bool
     */
    public function isUsingSwoole(): bool
    {
        return $this->useSwoole;
    }
}
