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
 * Message Hook Points - Defines when interceptors are invoked
 * 
 * Reference: Java MessageHookPoints enum
 */
class MessageHookPoints {
    /**
     * Before sending message
     */
    const SEND_BEFORE = 1;
    
    /**
     * After sending message
     */
    const SEND_AFTER = 2;
    
    /**
     * Before consuming message
     */
    const CONSUME_BEFORE = 3;
    
    /**
     * After consuming message
     */
    const CONSUME_AFTER = 4;
    
    /**
     * Get hook point name
     * 
     * @param int $hookPoint Hook point constant
     * @return string Hook point name
     */
    public static function getName(int $hookPoint): string {
        $names = [
            self::SEND_BEFORE => 'SEND_BEFORE',
            self::SEND_AFTER => 'SEND_AFTER',
            self::CONSUME_BEFORE => 'CONSUME_BEFORE',
            self::CONSUME_AFTER => 'CONSUME_AFTER',
        ];
        
        return $names[$hookPoint] ?? 'UNKNOWN';
    }
}
