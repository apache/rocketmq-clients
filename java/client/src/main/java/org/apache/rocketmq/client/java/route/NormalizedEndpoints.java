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

package org.apache.rocketmq.client.java.route;

import com.google.common.base.Objects;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * A normalized version of Endpoints that ensures consistent equality and hashing
 * regardless of the order of addresses. This is used as a key in connection pools
 * to ensure that endpoints with the same addresses (but in different order)
 * share the same connection.
 */
public class NormalizedEndpoints {
    private final AddressScheme scheme;
    private final List<Address> sortedAddresses;
    private final int hashCode;

    public NormalizedEndpoints(Endpoints endpoints) {
        this.scheme = endpoints.getScheme();
        // Create a sorted copy of addresses for consistent ordering
        this.sortedAddresses = new ArrayList<>(endpoints.getAddresses());
        this.sortedAddresses.sort(new AddressComparator());

        // Pre-compute hash code for better performance
        this.hashCode = computeHashCode();
    }

    public AddressScheme getScheme() {
        return scheme;
    }

    public List<Address> getSortedAddresses() {
        return sortedAddresses;
    }

    /**
     * Convert back to original Endpoints object.
     * Note: This will use the sorted order of addresses.
     */
    public Endpoints toEndpoints() {
        return new Endpoints(scheme, sortedAddresses);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NormalizedEndpoints that = (NormalizedEndpoints) o;
        return scheme == that.scheme &&
            Objects.equal(sortedAddresses, that.sortedAddresses);
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    private int computeHashCode() {
        return Objects.hashCode(scheme, sortedAddresses);
    }

    @Override
    public String toString() {
        // 预估容量，避免扩容
        StringBuilder sb = new StringBuilder(64 + sortedAddresses.size() * 32);
        sb.append("NormalizedEndpoints{scheme=").append(scheme).append(", sortedAddresses=[");
        for (int i = 0; i < sortedAddresses.size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            Address addr = sortedAddresses.get(i);
            sb.append(addr.getHost()).append(':').append(addr.getPort());
        }
        sb.append("]}");
        return sb.toString();
    }

    /**
     * Comparator for Address objects to ensure consistent ordering.
     * Compares by host first, then by port.
     */
    private static class AddressComparator implements Comparator<Address>, java.io.Serializable {
        private static final long serialVersionUID = 1L;

        @Override
        public int compare(Address a1, Address a2) {
            int hostComparison = a1.getHost().compareTo(a2.getHost());
            if (hostComparison != 0) {
                return hostComparison;
            }
            return Integer.compare(a1.getPort(), a2.getPort());
        }
    }
}
