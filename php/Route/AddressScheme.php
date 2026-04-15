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
 * AddressScheme defines the addressing scheme
 * 
 * References Java AddressScheme implementation
 */
class AddressScheme
{
    const IPv4 = 'IPv4';
    const IPv6 = 'IPv6';
    const DOMAIN_NAME = 'DOMAIN_NAME';
    
    /**
     * Get prefix for scheme
     * 
     * @param string $scheme Address scheme
     * @return string Prefix
     */
    public static function getPrefix(string $scheme): string
    {
        switch ($scheme) {
            case self::IPv4:
            case self::IPv6:
                return '';
            case self::DOMAIN_NAME:
                return 'dns:';
            default:
                return '';
        }
    }
    
    /**
     * Convert to Protobuf
     * 
     * @param string $scheme Address scheme
     * @return int Protobuf scheme value
     */
    public static function toProtobuf(string $scheme): int
    {
        switch ($scheme) {
            case self::IPv4:
                return \Apache\Rocketmq\V2\AddressScheme::IPv4;
            case self::IPv6:
                return \Apache\Rocketmq\V2\AddressScheme::IPv6;
            case self::DOMAIN_NAME:
                return \Apache\Rocketmq\V2\AddressScheme::DOMAIN_NAME;
            default:
                return \Apache\Rocketmq\V2\AddressScheme::DOMAIN_NAME;
        }
    }
    
    /**
     * Convert from Protobuf
     * 
     * @param int $scheme Protobuf scheme value
     * @return string Address scheme
     */
    public static function fromProtobuf(int $scheme): string
    {
        switch ($scheme) {
            case \Apache\Rocketmq\V2\AddressScheme::IPv4:
                return self::IPv4;
            case \Apache\Rocketmq\V2\AddressScheme::IPv6:
                return self::IPv6;
            case \Apache\Rocketmq\V2\AddressScheme::DOMAIN_NAME:
                return self::DOMAIN_NAME;
            default:
                return self::DOMAIN_NAME;
        }
    }
}
