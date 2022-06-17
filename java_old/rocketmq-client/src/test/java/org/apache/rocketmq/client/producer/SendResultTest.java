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

import static org.testng.Assert.assertEquals;

import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.client.tools.TestBase;
import org.testng.annotations.Test;

public class SendResultTest extends TestBase {
    @Test
    public void testGetSendStatus() {
        final Endpoints endpoints = fakeEndpoints0();
        final SendResult sendResult = new SendResult(endpoints, "msgId");
        assertEquals(sendResult.getSendStatus(), SendStatus.SEND_OK);
    }

    @Test
    public void testGetMsgId() {
        final Endpoints endpoints = fakeEndpoints0();
        final SendResult sendResult = new SendResult(endpoints, "msgId");
        assertEquals(sendResult.getMsgId(), "msgId");
    }

    @Test
    public void testGetTransactionId() {
        final Endpoints endpoints = fakeEndpoints0();
        final SendResult sendResult = new SendResult(endpoints, "msgId", "TransactionId");
        assertEquals(sendResult.getTransactionId(), "TransactionId");
    }

    @Test
    public void testGetEndpoints() {
        final Endpoints endpoints = fakeEndpoints0();
        final SendResult sendResult = new SendResult(endpoints, "msgId", "TransactionId");
        assertEquals(sendResult.getEndpoints(), endpoints);
    }
}