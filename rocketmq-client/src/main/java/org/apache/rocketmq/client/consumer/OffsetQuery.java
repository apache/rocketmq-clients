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

import com.google.common.base.MoreObjects;
import org.apache.rocketmq.client.message.MessageQueue;

public class OffsetQuery {
    final MessageQueue messageQueue;
    final QueryOffsetPolicy queryOffsetPolicy;
    final long timePoint;

    public OffsetQuery(MessageQueue messageQueue, QueryOffsetPolicy queryOffsetPolicy, long timePoint) {
        this.messageQueue = messageQueue;
        this.queryOffsetPolicy = queryOffsetPolicy;
        this.timePoint = timePoint;
    }

    public MessageQueue getMessageQueue() {
        return this.messageQueue;
    }

    public QueryOffsetPolicy getQueryOffsetPolicy() {
        return this.queryOffsetPolicy;
    }

    public long getTimePoint() {
        return this.timePoint;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("messageQueue", messageQueue)
                          .add("queryOffsetPolicy", queryOffsetPolicy)
                          .add("timePoint", timePoint)
                          .toString();
    }
}
