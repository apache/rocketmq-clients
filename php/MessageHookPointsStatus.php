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
 * Message Hook Points Status - Status of message processing
 * 
 * Reference: Java MessageHookPointsStatus enum
 */
class MessageHookPointsStatus {
    /**
     * Processing succeeded
     */
    const OK = 1;
    
    /**
     * Processing failed
     */
    const ERROR = 2;
    
    /**
     * Get status name
     * 
     * @param int $status Status constant
     * @return string Status name
     */
    public static function getName(int $status): string {
        $names = [
            self::OK => 'OK',
            self::ERROR => 'ERROR',
        ];
        
        return $names[$status] ?? 'UNKNOWN';
    }
}
