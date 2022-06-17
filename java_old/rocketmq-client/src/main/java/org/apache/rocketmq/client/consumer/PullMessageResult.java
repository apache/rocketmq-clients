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

import java.util.List;
import org.apache.rocketmq.client.message.MessageExt;

@SuppressWarnings("unused")
public class PullMessageResult {
    private final PullStatus pullStatus;
    private final long nextBeginOffset;
    private final long minOffset;
    private final long maxOffset;
    private final List<MessageExt> messagesFound;

    public PullMessageResult(PullStatus pullStatus, long nextBeginOffset, long minOffset, long maxOffset,
                             List<MessageExt> messagesFound) {
        this.pullStatus = pullStatus;
        this.nextBeginOffset = nextBeginOffset;
        this.minOffset = minOffset;
        this.maxOffset = maxOffset;
        this.messagesFound = messagesFound;
    }

    public PullStatus getPullStatus() {
        return this.pullStatus;
    }

    public long getNextBeginOffset() {
        return this.nextBeginOffset;
    }

    public long getMinOffset() {
        return this.minOffset;
    }

    public long getMaxOffset() {
        return this.maxOffset;
    }

    public List<MessageExt> getMessagesFound() {
        return this.messagesFound;
    }
}
