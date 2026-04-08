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
 * Client state enumeration
 * 
 * Defines the lifecycle states of Producer and Consumer
 * Refer to Java client's AbstractIdleService state machine implementation
 * 
 * State transition flow:
 * CREATED → STARTING → RUNNING → STOPPING → TERMINATED
 *                    ↓
 *                 FAILED
 */
class ClientState
{
    /**
     * Created - Initial state, object instantiated but not started
     */
    const CREATED = 'CREATED';
    
    /**
     * Starting - Performing initialization operations (connection verification, route query, etc.)
     */
    const STARTING = 'STARTING';
    
    /**
     * Running - Normal working state, can send/receive messages
     */
    const RUNNING = 'RUNNING';
    
    /**
     * Stopping - Performing cleanup operations (closing connections, releasing resources, etc.)
     */
    const STOPPING = 'STOPPING';
    
    /**
     * Terminated - Completely closed, cannot be used anymore
     */
    const TERMINATED = 'TERMINATED';
    
    /**
     * Failed - Error occurred during startup or runtime
     */
    const FAILED = 'FAILED';
    
    /**
     * Check if state allows operation execution
     * 
     * @param string $currentState Current state
     * @param array $allowedStates Allowed state list
     * @param string $operation Operation name (for error message)
     * @return void
     * @throws \Exception If state does not allow operation
     */
    public static function checkState($currentState, $allowedStates, $operation)
    {
        if (!in_array($currentState, $allowedStates)) {
            $allowedStr = implode(', ', $allowedStates);
            throw new \Exception(
                "Cannot {$operation} when client is in state: {$currentState}. " .
                "Allowed states: [{$allowedStr}]"
            );
        }
    }
    
    /**
     * Check if state can transition to target state
     * 
     * @param string $fromState Source state
     * @param string $toState Target state
     * @return bool Whether transition is allowed
     */
    public static function canTransition($fromState, $toState)
    {
        $validTransitions = [
            self::CREATED => [self::STARTING],
            self::STARTING => [self::RUNNING, self::FAILED],
            self::RUNNING => [self::STOPPING],
            self::STOPPING => [self::TERMINATED],
            self::FAILED => [], // Cannot transition after failure
            self::TERMINATED => [], // Cannot transition after termination
        ];
        
        return isset($validTransitions[$fromState]) && 
               in_array($toState, $validTransitions[$fromState]);
    }
    
    /**
     * Get state description
     * 
     * @param string $state State
     * @return string State description
     */
    public static function getDescription($state)
    {
        $descriptions = [
            self::CREATED => 'Created',
            self::STARTING => 'Starting',
            self::RUNNING => 'Running',
            self::STOPPING => 'Stopping',
            self::TERMINATED => 'Terminated',
            self::FAILED => 'Failed',
        ];
        
        return $descriptions[$state] ?? 'Unknown state';
    }
    
    /**
     * Check if it is a terminal state
     * 
     * @param string $state State
     * @return bool Whether it is a terminal state
     */
    public static function isTerminalState($state)
    {
        return in_array($state, [self::TERMINATED, self::FAILED]);
    }
    
    /**
     * Check if operation can be executed
     * 
     * @param string $state State
     * @return bool Whether operation can be executed
     */
    public static function isActive($state)
    {
        return $state === self::RUNNING;
    }
}
