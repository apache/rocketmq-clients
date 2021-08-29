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

package org.apache.rocketmq.client.exception;

public enum ErrorCode {
    NOT_STARTED,
    STARTED_BEFORE,

    STS_TOKEN_GET_FAILURE,
    /**
     *
     */
    FETCH_TOPIC_ROUTE_FAILURE,
    /**
     * If topic was not found or partition is empty.
     */
    TOPIC_NOT_FOUND,
    /**
     *
     */
    NO_AVAILABLE_NAME_SERVER,
    NO_PERMISSION,

    FETCH_NAME_SERVER_FAILURE,
    SIGNATURE_FAILURE,

    NO_LISTENER_REGISTERED,
    NOT_SUPPORTED_OPERATION,
    NO_ASSIGNMENT,
    ILLEGAL_FORMAT,
    SEEK_OFFSET_FAILURE,
    OTHER;
}
