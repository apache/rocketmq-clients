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

package org.apache.rocketmq.client.misc;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.rocketmq.client.route.Address;
import org.apache.rocketmq.client.route.AddressScheme;
import org.apache.rocketmq.client.route.Endpoints;
import org.apache.rocketmq.utility.HttpTinyClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TopAddressing {
    private static final Logger log = LoggerFactory.getLogger(TopAddressing.class);
    
    private static final int HTTP_TIMEOUT_MILLIS = 3 * 1000;
    private static final String DEFAULT_NAME_SERVER_DOMAIN = "jmenv.tbsite.net";
    private static final String DEFAULT_NAME_SERVER_SUB_GROUP = "nsaddr";

    private final String wsAddress;
    /**
     * TODO: supply unitName here.
     */
    private String unitName;

    public TopAddressing() {
        this.wsAddress = getWsAddress();
    }

    private String getWsAddress() {
        return MixAll.HTTP_PREFIX + DEFAULT_NAME_SERVER_DOMAIN + ":8080/rocketmq/" + DEFAULT_NAME_SERVER_SUB_GROUP;
    }

    public List<Endpoints> fetchNameServerAddresses() throws IOException {
        List<Endpoints> endpointsList = new ArrayList<Endpoints>();

        final HttpTinyClient.HttpResult httpResult = HttpTinyClient.httpGet(wsAddress, HTTP_TIMEOUT_MILLIS);
        if (httpResult.isOk()) {
            // TODO: check result format here.
            final String content = httpResult.getContent();
            // TODO: check split result here.
            final String[] nameServerAddresses = content.split(";");

            for (String nameServerAddress : nameServerAddresses) {
                List<Address> addresses = new ArrayList<Address>();
                final String[] split = nameServerAddress.split(":");
                String host = split[0];
                final int port = Integer.parseInt(split[1]);
                addresses.add(new Address(host, port));
                endpointsList.add(new Endpoints(AddressScheme.IPv4, addresses));
            }
            return endpointsList;
        }
        log.error("Failed to name server addresses from topAddress, httpCode={}", httpResult.getCode());
        return endpointsList;
    }
}
