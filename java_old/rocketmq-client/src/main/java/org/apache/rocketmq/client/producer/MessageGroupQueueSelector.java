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

package org.apache.rocketmq.client.producer;

import com.google.common.math.IntMath;
import java.util.List;
import org.apache.rocketmq.client.message.Message;
import org.apache.rocketmq.client.message.MessageQueue;

public class MessageGroupQueueSelector implements MessageQueueSelector {
    private final String messageGroup;

    public MessageGroupQueueSelector(String messageGroup) {
        this.messageGroup = messageGroup;
    }

    @Override
    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
        final int hashCode = messageGroup.hashCode();
        return mqs.get(IntMath.mod(hashCode, mqs.size()));
    }
}
