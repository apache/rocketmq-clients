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

package org.apache.rocketmq.client.remoting;

import apache.rocketmq.v1.Message;
import apache.rocketmq.v1.Resource;
import apache.rocketmq.v1.SendMessageRequest;
import apache.rocketmq.v1.SystemAttribute;
import com.google.protobuf.ByteString;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.client.impl.ClientConfig;
import org.apache.rocketmq.client.route.Address;
import org.apache.rocketmq.client.route.AddressScheme;
import org.apache.rocketmq.client.route.Endpoints;
import org.testng.annotations.Test;

public class RpcClientImplTest {

    private ClientConfig clientConfig = new ClientConfig("");

    @Test
    public void testQueryRoute() throws SSLException {
        //        List<Address> addresses = new ArrayList<Address>();
        //        addresses.add(new Address("11.165.223.199", 9876));
        //        final Endpoints endpoints = new Endpoints(AddressScheme.IPv4, addresses);
        //
        //        final RpcClientImpl rpcClient = new RpcClientImpl(new RpcTarget(endpoints, false));
        //
        //        Resource topicResource = Resource.newBuilder().setName("yc001").build();
        //
        //        QueryRouteRequest request =
        //                QueryRouteRequest.newBuilder().setTopic(topicResource).build();
        //        final QueryRouteResponse response = rpcClient.queryRoute(request, 3, TimeUnit.SECONDS);
        //        System.out.println(response);
    }

    @Test
    public void testSendMessage() throws UnsupportedEncodingException, SSLException {

        List<Address> addresses = new ArrayList<Address>();
        addresses.add(new Address("11.158.159.57", 8081));
        final Endpoints endpoints = new Endpoints(AddressScheme.IPv4, addresses);
        final RpcClientImpl rpcClient = new RpcClientImpl(endpoints);

        final Resource topicResource = Resource.newBuilder().setArn("MQ_INST_1973281269661160_BXmPlOA6").setName(
                "yc001").build();
        SystemAttribute systemAttribute = SystemAttribute.newBuilder().setMessageId("sdfsdfsdf").build();
        Message msg = Message.newBuilder().setTopic(topicResource).setSystemAttribute(systemAttribute)
                             .setBody(ByteString.copyFrom("Hello", "UTF-8")).build();

        SendMessageRequest request =
                SendMessageRequest.newBuilder().setMessage(msg).build();

        // final SendMessageResponse response = rpcClient.sendMessage(new Metadata(), request, 3, TimeUnit.SECONDS);
        // System.out.println(request);
    }

    @Test
    public void testQueryAssignment() throws SSLException {
    }

}