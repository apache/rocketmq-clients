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

package org.apache.rocketmq.client.message;

public enum MessageHookPoint {
    /**
     * Hook point before sending.
     */
    PRE_SEND_MESSAGE,
    /**
     * Hook point after sending.
     */
    POST_SEND_MESSAGE,

    /**
     * Hook point before pull message.
     */
    PRE_PULL_MESSAGE,
    /**
     * Hook point after pull message.
     */
    POST_PULL_MESSAGE,

    /**
     * Hook point before message consumption.
     */
    PRE_MESSAGE_CONSUMPTION,
    /**
     * Hook point after message consumption.
     */
    POST_MESSAGE_CONSUMPTION,

    /**
     * Hook point before end the transaction message.
     */
    PRE_END_MESSAGE,
    /**
     * Hook point after end the transaction message.
     */
    POST_END_MESSAGE;

    public enum PointStatus {
        /**
         * Default status.
         */
        UNSET,
        /**
         * Success.
         */
        OK,
        /**
         * Failure.
         */
        ERROR;
    }
}
