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
 * Invocation Status - Status of metric invocation
 * 
 * Reference: Java InvocationStatus enum
 */
class InvocationStatus {
    /**
     * Invocation succeeded
     */
    const SUCCESS = 'success';
    
    /**
     * Invocation failed
     */
    const FAILURE = 'failure';
    
    /**
     * Get all status values
     * 
     * @return array Array of status values
     */
    public static function getAll(): array {
        return [
            self::SUCCESS,
            self::FAILURE,
        ];
    }
    
    /**
     * Check if status is valid
     * 
     * @param string $status Status to check
     * @return bool True if valid
     */
    public static function isValid(string $status): bool {
        return in_array($status, [self::SUCCESS, self::FAILURE], true);
    }
}
