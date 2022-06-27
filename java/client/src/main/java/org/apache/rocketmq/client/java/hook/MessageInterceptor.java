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

package org.apache.rocketmq.client.java.hook;

import java.time.Duration;
import java.util.List;
import org.apache.rocketmq.client.java.message.MessageCommon;

/**
 * Message interceptor based on {@link MessageHookPoints}.
 */
public interface MessageInterceptor {
    /**
     * Do something before {@link MessageHookPoints}.
     *
     * @param messageHookPoints message hook points.
     * @param messageCommons    list of message commons.
     */
    void doBefore(MessageHookPoints messageHookPoints, List<MessageCommon> messageCommons);

    /**
     * Do something after {@link MessageHookPoints}.
     *
     * @param messageHookPoints message hook points.
     * @param messageCommons    list of message commons.
     * @param duration          duration of the hook points.
     * @param status            status of operation of the hook points.
     */
    void doAfter(MessageHookPoints messageHookPoints, List<MessageCommon> messageCommons, Duration duration,
        MessageHookPointsStatus status);
}
