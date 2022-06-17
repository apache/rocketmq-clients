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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;

import apache.rocketmq.v1.Permission;
import org.apache.rocketmq.client.route.Partition;
import org.apache.rocketmq.client.tools.TestBase;
import org.testng.annotations.Test;

public class MessageQueueTest extends TestBase {

    private final MessageQueue mq0;
    private final MessageQueue mq1;
    private final MessageQueue mq2;

    public MessageQueueTest() {
        final Partition partition = fakePartition0();
        this.mq0 = new MessageQueue(partition);
        this.mq1 = new MessageQueue(mq0.getTopic(), mq0.getBrokerName(), mq0.getQueueId());
        final apache.rocketmq.v1.Partition pbPartition = fakePbPartition0(Permission.NONE);
        this.mq2 = new MessageQueue(new Partition(pbPartition));
    }

    @Test
    public void testEquals() {
        assertEquals(mq0, mq1);
        assertEquals(mq2, mq1);
        assertNotEquals(mq0, mq2);
    }

    @Test
    public void testHashCode() {
        assertEquals(mq0.hashCode(), mq1.hashCode());
        assertEquals(mq2.hashCode(), mq1.hashCode());
        assertEquals(mq0.hashCode(), mq2.hashCode());
    }
}