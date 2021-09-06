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
     * Hook point before pull.
     */
    PRE_PULL,
    /**
     * Hook point after pull.
     */
    POST_PULL,
    /**
     * Hook point before receive.
     */
    PRE_RECEIVE,
    /**
     * Hook point after receive.
     */
    POST_RECEIVE,
    /**
     * Hook point before message consumption.
     */
    PRE_MESSAGE_CONSUMPTION,
    /**
     * Hook point after message consumption.
     */
    POST_MESSAGE_CONSUMPTION,
    /**
     * Hook point before ack message.
     */
    PRE_ACK_MESSAGE,
    /**
     * Hook point before ack message.
     */
    POST_ACK_MESSAGE,
    /**
     * Hook point before nack message.
     */
    PRE_NACK_MESSAGE,
    /**
     * Hook point before nack message.
     */
    POST_NACK_MESSAGE,
    /**
     * Hook point before commit transaction message.
     */
    PRE_COMMIT_MESSAGE,
    /**
     * Hook point after commit message.
     */
    POST_COMMIT_MESSAGE,
    /**
     * Hook point before rollback transaction message.
     */
    PRE_ROLLBACK_MESSAGE,
    /**
     * Hook point after rollback transaction message.
     */
    POST_ROLLBACK_MESSAGE,
    /**
     * Hook point before forward message to DLQ;
     */
    PRE_FORWARD_MESSAGE_TO_DLQ,
    /**
     * Hook point after forward message to DLQ;
     */
    POST_FORWARD_MESSAGE_TO_DLQ
}
