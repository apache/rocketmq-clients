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

package org.apache.rocketmq.client.impl.consumer;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.rocketmq.client.impl.ClientManager;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.tools.TestBase;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class DefaultMQPushConsumerImplTest extends TestBase {
    @Mock
    private ClientManager clientManager;

    @InjectMocks
    private final DefaultMQPushConsumerImpl consumerImpl = new DefaultMQPushConsumerImpl(dummyGroup0);

    @BeforeMethod
    public void beforeMethod() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void setOffsetStoreWithNull() {
        try {
            consumerImpl.setOffsetStore(null);
            fail();
        } catch (NullPointerException ignore) {
            // Ignore on purpose.
        }
    }

    @Test
    public void testHasCustomOffsetStore() {
        OffsetStore offsetStore = new OffsetStore() {
            @Override
            public void start() {
            }

            @Override
            public void shutdown() {
            }

            @Override
            public void updateOffset(MessageQueue mq, long offset) {
            }

            @Override
            public long readOffset(MessageQueue mq) {
                return 0;
            }
        };
        consumerImpl.setOffsetStore(offsetStore);
        assertTrue(consumerImpl.hasCustomOffsetStore());
    }
}
