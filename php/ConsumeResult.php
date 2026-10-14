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
 * ConsumeResult - Result returned by the user message listener callback.
 *
 * PHP 8.1 backed enum replacing class constants.
 */
enum ConsumeResult: int
{
    case SUCCESS = 0;
    case FAILURE = 1;

    /**
     * Normalize a callback result to a ConsumeResult enum case.
     *
     * Accepts int (0=SUCCESS, 1=FAILURE) or ConsumeResult enum.
     *
     * @param mixed $result Integer or ConsumeResult enum
     * @return self
     */
    public static function fromMixed(mixed $result): self
    {
        if ($result instanceof self) {
            return $result;
        }
        if (is_int($result)) {
            return self::from($result);
        }
        // Default to SUCCESS for truthy, FAILURE for falsy
        return $result ? self::SUCCESS : self::FAILURE;
    }
}
