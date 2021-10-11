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

package org.apache.rocketmq.client.impl;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import apache.rocketmq.v1.HeartbeatRequest;
import apache.rocketmq.v1.NotifyClientTerminationRequest;
import apache.rocketmq.v1.PollCommandRequest;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.route.TopicRouteData;
import org.apache.rocketmq.client.tools.TestBase;
import org.testng.annotations.Test;

public class ClientImplTest extends TestBase {
    private final ClientImpl client;

    public ClientImplTest() throws ClientException {
        this.client = new ClientImpl(FAKE_GROUP_0) {
            @Override
            public void onTopicRouteDataUpdate0(String topic, TopicRouteData topicRouteData) {
            }

            @Override
            public NotifyClientTerminationRequest wrapNotifyClientTerminationRequest() {
                return null;
            }

            @Override
            public HeartbeatRequest wrapHeartbeatRequest() {
                return null;
            }

            @Override
            public PollCommandRequest wrapPollCommandRequest() {
                return null;
            }

            @Override
            public void doHealthCheck() {
            }

            @Override
            public void doStats() {
            }
        };
    }

    @Test
    public void testSetNamesrvAddr() throws ClientException {
        String nameServerEndpoint = "127.0.0.1:9876";
        client.setNamesrvAddr(nameServerEndpoint);
        assertEquals(client.getNamespace(), "");
        assertEquals(client.getRegionId(), ClientConfig.DEFAULT_REGION_ID);

        nameServerEndpoint = "foobar";
        try {
            client.setNamesrvAddr(nameServerEndpoint);
            fail();
        } catch (ClientException ignore) {
            // ignore on purpose.
        }

        client.setNamespace("");
        nameServerEndpoint = "http://onsaddr.cn-hangzhou.mq-internal.aliyuncs.com:8080";
        client.setNamesrvAddr(nameServerEndpoint);
        assertEquals(client.getNamespace(), "");
        assertEquals(client.getRegionId(), "cn-hangzhou");

        client.setNamespace("");
        nameServerEndpoint = "http://onsaddr.cn-hangzhou.mq-internal.aliyuncs.com";
        client.setNamesrvAddr(nameServerEndpoint);
        assertEquals(client.getNamespace(), "");
        assertEquals(client.getRegionId(), "cn-hangzhou");

        client.setNamespace("");
        nameServerEndpoint = "https://onsaddr.cn-hangzhou.mq-internal.aliyuncs.com:8080";
        client.setNamesrvAddr(nameServerEndpoint);
        assertEquals(client.getNamespace(), "");
        assertEquals(client.getRegionId(), "cn-hangzhou");

        client.setNamespace("");
        nameServerEndpoint = "http://MQ_INST_1080056302921134_BXQdPCN6.mq-internet-access.mq-internet.aliyuncs.com:80";
        client.setNamesrvAddr(nameServerEndpoint);
        assertEquals(client.getNamespace(), "MQ_INST_1080056302921134_BXQdPCN6");
        assertEquals(client.getRegionId(), "mq-internet-access");

        client.setNamespace("");
        nameServerEndpoint = "https://MQ_INST_1080056302921134_BXQdPCN6.mq-internet-access.mq-internet.aliyuncs.com:80";
        client.setNamesrvAddr(nameServerEndpoint);
        assertEquals(client.getNamespace(), "MQ_INST_1080056302921134_BXQdPCN6");
        assertEquals(client.getRegionId(), "mq-internet-access");

        client.setNamespace(FAKE_ARN_0);
        nameServerEndpoint = "https://MQ_INST_1080056302921134_BXQdPCN6.mq-internet-access.mq-internet.aliyuncs.com:80";
        client.setNamesrvAddr(nameServerEndpoint);
        assertEquals(client.getNamespace(), FAKE_ARN_0);
        assertEquals(client.getRegionId(), "mq-internet-access");
    }
}