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

public enum PullStatus {
    /**
     * Messages are received as expected or no new message arrived.
     */
    OK,
    /**
     * Deadline expired before matched messages are found in the server side.
     */
    DEADLINE_EXCEEDED,
    /**
     * Resource has been exhausted, perhaps a per-user quota. For example, too many receive-message requests are
     * submitted to the same partition at the same time.
     */
    RESOURCE_EXHAUSTED,
    /**
     * The target partition does not exist, which might have been deleted.
     */
    NOT_FOUND,
    /**
     * Receive-message operation was attempted past the valid range. For pull operation, clients may try to pull expired
     * messages.
     */
    OUT_OF_RANGE,
    /**
     * Serious errors occurred in the server side.
     */
    INTERNAL
}
