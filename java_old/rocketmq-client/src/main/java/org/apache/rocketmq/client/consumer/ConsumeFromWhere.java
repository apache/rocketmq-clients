/*
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

package org.apache.rocketmq.client.consumer;

public enum ConsumeFromWhere {
    /**
     * From the latest offset.
     */
    CONSUME_FROM_MAX_OFFSET,
    /**
     * From the first offset exists.
     */
    CONSUME_FROM_FIRST_OFFSET,
    /**
     * Seek the offset by timestamp.
     */
    CONSUME_FROM_TIMESTAMP,

    /**
     * From the last offset recorded, if last offset does not exist, {@link #CONSUME_FROM_MAX_OFFSET} would be applied.
     */
    CONSUME_FROM_LAST_OFFSET,
}
