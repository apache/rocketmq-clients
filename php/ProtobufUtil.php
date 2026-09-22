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
 * ProtobufUtil - Safe utilities for Google Protobuf RepeatedField/MapField.
 *
 * Protobuf's RepeatedField and MapField are NOT native PHP arrays.
 * `empty()` on them always returns false (they are objects).
 * `count()` works but can be unreliable with older protobuf extension versions.
 * These helpers convert safely to native PHP arrays.
 */
class ProtobufUtil
{
    /**
     * Convert a RepeatedField to a native PHP array.
     * Uses iterator-based access to avoid the offsetGet bug in protobuf
     * where accessing an empty field triggers "Undefined array key" warnings.
     *
     * @param mixed $repeatedField Protobuf RepeatedField or null
     * @return array
     */
    public static function repeatedFieldToArray($repeatedField): array
    {
        if ($repeatedField === null) {
            return [];
        }
        if (is_array($repeatedField)) {
            return $repeatedField;
        }
        // Use iterator — RepeatedField implements \IteratorAggregate
        if ($repeatedField instanceof \Traversable) {
            $result = [];
            foreach ($repeatedField as $key => $value) {
                $result[$key] = $value;
            }
            return $result;
        }
        return (array) $repeatedField;
    }

    /**
     * Safely check if a RepeatedField is empty.
     * Replaces `empty($repeatedField)` which always returns false for objects.
     *
     * @param mixed $repeatedField Protobuf RepeatedField or null
     * @return bool
     */
    public static function isRepeatedFieldEmpty($repeatedField): bool
    {
        if ($repeatedField === null) {
            return true;
        }
        if (is_array($repeatedField)) {
            return empty($repeatedField);
        }
        if ($repeatedField instanceof \Countable) {
            return $repeatedField->count() === 0;
        }
        return true;
    }

    /**
     * Safely count elements in a RepeatedField.
     * More reliable than `count($repeatedField)` across protobuf versions.
     *
     * @param mixed $repeatedField Protobuf RepeatedField or null
     * @return int
     */
    public static function countRepeatedField($repeatedField): int
    {
        if ($repeatedField === null) {
            return 0;
        }
        if (is_array($repeatedField)) {
            return count($repeatedField);
        }
        if ($repeatedField instanceof \Countable) {
            return $repeatedField->count();
        }
        return 0;
    }

    /**
     * Convert a MapField to a native PHP associative array.
     *
     * @param mixed $mapField Protobuf MapField or null
     * @return array
     */
    public static function mapFieldToArray($mapField): array
    {
        if ($mapField === null) {
            return [];
        }
        if (is_array($mapField)) {
            return $mapField;
        }
        if ($mapField instanceof \Traversable) {
            $result = [];
            foreach ($mapField as $key => $value) {
                $result[$key] = $value;
            }
            return $result;
        }
        return (array) $mapField;
    }

    /**
     * Safely check if a MapField is empty.
     *
     * @param mixed $mapField Protobuf MapField or null
     * @return bool
     */
    public static function isMapFieldEmpty($mapField): bool
    {
        if ($mapField === null) {
            return true;
        }
        if (is_array($mapField)) {
            return empty($mapField);
        }
        if ($mapField instanceof \Countable) {
            return $mapField->count() === 0;
        }
        return true;
    }
}
