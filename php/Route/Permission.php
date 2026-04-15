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

namespace Apache\Rocketmq\Route;

/**
 * Permission defines message queue access permissions
 * 
 * References Java Permission enum (93 lines)
 */
class Permission
{
    const NONE = 0;
    const READ = 1;
    const WRITE = 2;
    const READ_WRITE = 3;
    
    /**
     * Check if permission is writable
     * 
     * @param int $permission Permission value
     * @return bool Whether writable
     */
    public static function isWritable(int $permission): bool
    {
        return $permission === self::WRITE || $permission === self::READ_WRITE;
    }
    
    /**
     * Check if permission is readable
     * 
     * @param int $permission Permission value
     * @return bool Whether readable
     */
    public static function isReadable(int $permission): bool
    {
        return $permission === self::READ || $permission === self::READ_WRITE;
    }
    
    /**
     * Convert from Protobuf
     * 
     * @param int $permission Protobuf permission value
     * @return int Permission value
     * @throws \InvalidArgumentException If invalid permission
     */
    public static function fromProtobuf(int $permission): int
    {
        switch ($permission) {
            case \Apache\Rocketmq\V2\Permission::READ:
                return self::READ;
            case \Apache\Rocketmq\V2\Permission::WRITE:
                return self::WRITE;
            case \Apache\Rocketmq\V2\Permission::READ_WRITE:
                return self::READ_WRITE;
            case \Apache\Rocketmq\V2\Permission::NONE:
                return self::NONE;
            default:
                throw new \InvalidArgumentException("Message queue permission is not specified");
        }
    }
    
    /**
     * Convert to Protobuf
     * 
     * @param int $permission Permission value
     * @return int Protobuf permission value
     * @throws \InvalidArgumentException If invalid permission
     */
    public static function toProtobuf(int $permission): int
    {
        switch ($permission) {
            case self::READ:
                return \Apache\Rocketmq\V2\Permission::READ;
            case self::WRITE:
                return \Apache\Rocketmq\V2\Permission::WRITE;
            case self::READ_WRITE:
                return \Apache\Rocketmq\V2\Permission::READ_WRITE;
            case self::NONE:
                return \Apache\Rocketmq\V2\Permission::NONE;
            default:
                throw new \InvalidArgumentException("Message queue permission is not specified");
        }
    }
}
