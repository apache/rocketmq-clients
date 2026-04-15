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
 * GracefulShutdownHandler - Handles graceful shutdown using PCNTL signals
 * 
 * Reference Java graceful shutdown mechanism:
 * - Listens for SIGTERM, SIGINT signals
 * - Triggers orderly shutdown of consumer
 * - Waits for inflight requests to complete
 * - Ensures no message loss during shutdown
 */
class GracefulShutdownHandler
{
    /**
     * @var PushConsumerImpl Push consumer to shutdown
     */
    private $pushConsumer;
    
    /**
     * @var bool Whether handler is registered
     */
    private $registered = false;
    
    /**
     * @var callable Shutdown callback
     */
    private $shutdownCallback;
    
    /**
     * Constructor
     * 
     * @param PushConsumerImpl $pushConsumer Push consumer
     * @param callable|null $shutdownCallback Optional shutdown callback
     */
    public function __construct(PushConsumerImpl $pushConsumer, ?callable $shutdownCallback = null)
    {
        $this->pushConsumer = $pushConsumer;
        $this->shutdownCallback = $shutdownCallback;
    }
    
    /**
     * Register signal handlers for graceful shutdown
     * 
     * @return bool Whether registration was successful
     */
    public function register(): bool
    {
        // Check if pcntl extension is available
        if (!extension_loaded('pcntl')) {
            error_log("PCNTL extension is not loaded. Graceful shutdown via signals will not work.");
            error_log("Install PCNTL extension or use alternative shutdown mechanism.");
            return false;
        }
        
        if ($this->registered) {
            return true;
        }
        
        // Register SIGTERM handler (default termination signal)
        pcntl_signal(SIGTERM, function($signo) {
            $this->handleShutdownSignal($signo, 'SIGTERM');
        });
        
        // Register SIGINT handler (Ctrl+C)
        pcntl_signal(SIGINT, function($signo) {
            $this->handleShutdownSignal($signo, 'SIGINT');
        });
        
        // Register SIGQUIT handler (graceful quit)
        if (defined('SIGQUIT')) {
            pcntl_signal(SIGQUIT, function($signo) {
                $this->handleShutdownSignal($signo, 'SIGQUIT');
            });
        }
        
        $this->registered = true;
        
        error_log("Graceful shutdown handlers registered for SIGTERM, SIGINT" . 
            (defined('SIGQUIT') ? ", SIGQUIT" : ""));
        
        return true;
    }
    
    /**
     * Handle shutdown signal
     * 
     * @param int $signo Signal number
     * @param string $signalName Signal name
     * @return void
     */
    private function handleShutdownSignal(int $signo, string $signalName): void
    {
        error_log("Received {$signalName} signal ({$signo}), initiating graceful shutdown...");
        
        // Execute shutdown callback if registered
        if ($this->shutdownCallback !== null) {
            call_user_func($this->shutdownCallback, $signalName);
        }
        
        // Close the consumer (triggers graceful shutdown)
        try {
            $this->pushConsumer->close();
            error_log("PushConsumer closed gracefully");
        } catch (\Exception $e) {
            error_log("Error during graceful shutdown: " . $e->getMessage());
        }
        
        // Exit with success status
        exit(0);
    }
    
    /**
     * Dispatch signals (should be called periodically in main loop)
     * 
     * @return void
     */
    public function dispatchSignals(): void
    {
        if ($this->registered) {
            pcntl_signal_dispatch();
        }
    }
    
    /**
     * Unregister signal handlers
     * 
     * @return void
     */
    public function unregister(): void
    {
        if ($this->registered) {
            pcntl_signal(SIGTERM, SIG_DFL);
            pcntl_signal(SIGINT, SIG_DFL);
            
            if (defined('SIGQUIT')) {
                pcntl_signal(SIGQUIT, SIG_DFL);
            }
            
            $this->registered = false;
        }
    }
    
    /**
     * Check if handler is registered
     * 
     * @return bool
     */
    public function isRegistered(): bool
    {
        return $this->registered;
    }
}
