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

package org.apache.rocketmq.client.route;

import com.google.common.base.Preconditions;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;

@Getter
@EqualsAndHashCode
public class Endpoints {
    private static final String ADDRESS_SEPARATOR = ",";
    private final AddressScheme addressScheme;

    /**
     * URI path for grpc target, e.g:
     * <p>1. dns:rocketmq.apache.org:8080
     * <p>2. ipv4:127.0.0.1:10911,127.0.0.2:10912
     * <p>3. ipv6:1050:0000:0000:0000:0005:0600:300c:326b:10911,1050:0000:0000:0000:0005:0600:300c:326b:10912
     */
    private final String facade;
    private final List<Address> addresses;

    public Endpoints(AddressScheme addressScheme, List<Address> addresses) {
        // TODO: polish code here.
        if (AddressScheme.DOMAIN_NAME == addressScheme && addresses.size() > 1) {
            throw new UnsupportedOperationException("Multiple addresses not allowed in domain schema.");
        }
        Preconditions.checkNotNull(addresses);
        if (addresses.isEmpty()) {
            throw new UnsupportedOperationException("No available address");
        }

        this.addressScheme = addressScheme;
        this.addresses = addresses;
        StringBuilder facadeBuilder = new StringBuilder();
        facadeBuilder.append(addressScheme.getPrefix());
        for (Address address : addresses) {
            facadeBuilder.append(address.getAddress()).append(ADDRESS_SEPARATOR);
        }
        this.facade = facadeBuilder.substring(0, facadeBuilder.length() - 1);
    }

    public List<InetSocketAddress> convertToSocketAddresses() {
        switch (addressScheme) {
            case DOMAIN_NAME:
                return null;
            case IPv4:
            case IPv6:
            default:
                // Customize the name resolver to support multiple addresses.
                List<InetSocketAddress> socketAddresses = new ArrayList<InetSocketAddress>();
                for (Address address : addresses) {
                    socketAddresses.add(new InetSocketAddress(address.getHost(), address.getPort()));
                }
                return socketAddresses;
        }
    }

    @Override
    public String toString() {
        return facade;
    }
}
