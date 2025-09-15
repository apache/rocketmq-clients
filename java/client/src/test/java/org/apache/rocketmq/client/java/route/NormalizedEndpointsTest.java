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

import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class NormalizedEndpointsTest {

    @Test
    public void testSingleAddressNormalization() {
        Endpoints endpoints = new Endpoints("127.0.0.1:8080");
        NormalizedEndpoints normalized = new NormalizedEndpoints(endpoints);
        
        Assert.assertEquals(AddressScheme.IPv4, normalized.getScheme());
        Assert.assertEquals(1, normalized.getSortedAddresses().size());
        
        Address address = normalized.getSortedAddresses().get(0);
        Assert.assertEquals("127.0.0.1", address.getHost());
        Assert.assertEquals(8080, address.getPort());
    }

    @Test
    public void testMultipleAddressesOrderIndependence() {
        // Create endpoints with same addresses but different order
        Endpoints endpoints1 = new Endpoints("127.0.0.1:8080;127.0.0.2:8081;127.0.0.3:8082");
        Endpoints endpoints2 = new Endpoints("127.0.0.3:8082;127.0.0.1:8080;127.0.0.2:8081");
        Endpoints endpoints3 = new Endpoints("127.0.0.2:8081;127.0.0.3:8082;127.0.0.1:8080");
        
        NormalizedEndpoints normalized1 = new NormalizedEndpoints(endpoints1);
        NormalizedEndpoints normalized2 = new NormalizedEndpoints(endpoints2);
        NormalizedEndpoints normalized3 = new NormalizedEndpoints(endpoints3);
        
        // All should be equal
        Assert.assertEquals(normalized1, normalized2);
        Assert.assertEquals(normalized2, normalized3);
        Assert.assertEquals(normalized1, normalized3);
        
        // Hash codes should be equal
        Assert.assertEquals(normalized1.hashCode(), normalized2.hashCode());
        Assert.assertEquals(normalized2.hashCode(), normalized3.hashCode());
        
        // Sorted addresses should be in consistent order
        List<Address> sortedAddresses1 = normalized1.getSortedAddresses();
        List<Address> sortedAddresses2 = normalized2.getSortedAddresses();
        List<Address> sortedAddresses3 = normalized3.getSortedAddresses();
        
        Assert.assertEquals(sortedAddresses1, sortedAddresses2);
        Assert.assertEquals(sortedAddresses2, sortedAddresses3);
        
        // Verify the sorted order (should be lexicographic by host, then by port)
        Assert.assertEquals("127.0.0.1", sortedAddresses1.get(0).getHost());
        Assert.assertEquals(8080, sortedAddresses1.get(0).getPort());
        Assert.assertEquals("127.0.0.2", sortedAddresses1.get(1).getHost());
        Assert.assertEquals(8081, sortedAddresses1.get(1).getPort());
        Assert.assertEquals("127.0.0.3", sortedAddresses1.get(2).getHost());
        Assert.assertEquals(8082, sortedAddresses1.get(2).getPort());
    }

    @Test
    public void testAddressSorting() {
        // Test addresses with same host but different ports
        Address addr1 = new Address("127.0.0.1", 8080);
        Address addr2 = new Address("127.0.0.1", 8081);
        Address addr3 = new Address("127.0.0.2", 8080);
        
        // Create endpoints with unsorted addresses
        Endpoints endpoints = new Endpoints(AddressScheme.IPv4, Arrays.asList(addr3, addr2, addr1));
        NormalizedEndpoints normalized = new NormalizedEndpoints(endpoints);
        
        List<Address> sortedAddresses = normalized.getSortedAddresses();
        
        // Should be sorted by host first, then by port
        Assert.assertEquals("127.0.0.1", sortedAddresses.get(0).getHost());
        Assert.assertEquals(8080, sortedAddresses.get(0).getPort());
        Assert.assertEquals("127.0.0.1", sortedAddresses.get(1).getHost());
        Assert.assertEquals(8081, sortedAddresses.get(1).getPort());
        Assert.assertEquals("127.0.0.2", sortedAddresses.get(2).getHost());
        Assert.assertEquals(8080, sortedAddresses.get(2).getPort());
    }

    @Test
    public void testDifferentSchemesNotEqual() {
        // Create IPv4 and domain endpoints
        Endpoints ipv4Endpoints = new Endpoints("127.0.0.1:8080");
        Endpoints domainEndpoints = new Endpoints("example.com:8080");
        
        NormalizedEndpoints normalized1 = new NormalizedEndpoints(ipv4Endpoints);
        NormalizedEndpoints normalized2 = new NormalizedEndpoints(domainEndpoints);
        
        Assert.assertNotEquals(normalized1, normalized2);
        Assert.assertNotEquals(normalized1.hashCode(), normalized2.hashCode());
    }

    @Test
    public void testDifferentAddressesNotEqual() {
        Endpoints endpoints1 = new Endpoints("127.0.0.1:8080;127.0.0.2:8081");
        Endpoints endpoints2 = new Endpoints("127.0.0.1:8080;127.0.0.3:8081");
        
        NormalizedEndpoints normalized1 = new NormalizedEndpoints(endpoints1);
        NormalizedEndpoints normalized2 = new NormalizedEndpoints(endpoints2);
        
        Assert.assertNotEquals(normalized1, normalized2);
    }

    @Test
    public void testToEndpoints() {
        Endpoints original = new Endpoints("127.0.0.3:8082;127.0.0.1:8080;127.0.0.2:8081");
        NormalizedEndpoints normalized = new NormalizedEndpoints(original);
        
        Endpoints converted = normalized.toEndpoints();
        
        Assert.assertEquals(normalized.getScheme(), converted.getScheme());
        Assert.assertEquals(normalized.getSortedAddresses(), converted.getAddresses());
        
        // The converted endpoints should have addresses in sorted order
        List<Address> addresses = converted.getAddresses();
        Assert.assertEquals("127.0.0.1", addresses.get(0).getHost());
        Assert.assertEquals("127.0.0.2", addresses.get(1).getHost());
        Assert.assertEquals("127.0.0.3", addresses.get(2).getHost());
    }

    @Test
    public void testHashCodeConsistency() {
        Endpoints endpoints = new Endpoints("127.0.0.1:8080;127.0.0.2:8081");
        NormalizedEndpoints normalized1 = new NormalizedEndpoints(endpoints);
        NormalizedEndpoints normalized2 = new NormalizedEndpoints(endpoints);
        
        // Hash code should be consistent across multiple calls
        Assert.assertEquals(normalized1.hashCode(), normalized1.hashCode());
        Assert.assertEquals(normalized1.hashCode(), normalized2.hashCode());
    }

    @Test
    public void testEqualsContract() {
        Endpoints endpoints1 = new Endpoints("127.0.0.1:8080;127.0.0.2:8081");
        Endpoints endpoints2 = new Endpoints("127.0.0.2:8081;127.0.0.1:8080");
        
        NormalizedEndpoints normalized1 = new NormalizedEndpoints(endpoints1);
        NormalizedEndpoints normalized2 = new NormalizedEndpoints(endpoints2);
        
        // Reflexive
        Assert.assertEquals(normalized1, normalized1);
        
        // Symmetric
        Assert.assertEquals(normalized1, normalized2);
        Assert.assertEquals(normalized2, normalized1);
        
        // Transitive (already tested in testMultipleAddressesOrderIndependence)
        
        // Consistent
        Assert.assertEquals(normalized1, normalized2);
        Assert.assertEquals(normalized1, normalized2);
        
        // Non-null
        Assert.assertNotEquals(normalized1, null);
    }

    @Test
    public void testToString() {
        Endpoints endpoints = new Endpoints("127.0.0.2:8081;127.0.0.1:8080");
        NormalizedEndpoints normalized = new NormalizedEndpoints(endpoints);
        
        String toString = normalized.toString();
        
        // Should contain scheme and sorted addresses
        Assert.assertTrue(toString.contains("IPv4"));
        Assert.assertTrue(toString.contains("127.0.0.1:8080"));
        Assert.assertTrue(toString.contains("127.0.0.2:8081"));
        
        // Should be in sorted order (127.0.0.1 should come before 127.0.0.2)
        int index1 = toString.indexOf("127.0.0.1:8080");
        int index2 = toString.indexOf("127.0.0.2:8081");
        Assert.assertTrue("Addresses should be in sorted order in toString", index1 < index2);
    }

    @Test
    public void testIPv6Addresses() {
        Endpoints endpoints1 = new Endpoints("1050:0000:0000:0000:0005:0600:300c:326b:8080;" +
            "1050:0000:0000:0000:0005:0600:300c:326c:8081");
        Endpoints endpoints2 = new Endpoints("1050:0000:0000:0000:0005:0600:300c:326c:8081;" +
            "1050:0000:0000:0000:0005:0600:300c:326b:8080");
        
        NormalizedEndpoints normalized1 = new NormalizedEndpoints(endpoints1);
        NormalizedEndpoints normalized2 = new NormalizedEndpoints(endpoints2);
        
        Assert.assertEquals(normalized1, normalized2);
        Assert.assertEquals(AddressScheme.IPv6, normalized1.getScheme());
    }

    @Test
    public void testDomainName() {
        Endpoints endpoints = new Endpoints("example.com:8080");
        NormalizedEndpoints normalized = new NormalizedEndpoints(endpoints);
        
        Assert.assertEquals(AddressScheme.DOMAIN_NAME, normalized.getScheme());
        Assert.assertEquals(1, normalized.getSortedAddresses().size());
        
        Address address = normalized.getSortedAddresses().get(0);
        Assert.assertEquals("example.com", address.getHost());
        Assert.assertEquals(8080, address.getPort());
    }

    @Test
    public void testSamePortDifferentHosts() {
        Endpoints endpoints1 = new Endpoints("127.0.0.3:8080;127.0.0.1:8080;127.0.0.2:8080");
        Endpoints endpoints2 = new Endpoints("127.0.0.1:8080;127.0.0.3:8080;127.0.0.2:8080");
        
        NormalizedEndpoints normalized1 = new NormalizedEndpoints(endpoints1);
        NormalizedEndpoints normalized2 = new NormalizedEndpoints(endpoints2);
        
        Assert.assertEquals(normalized1, normalized2);
        
        // Verify sorted order by host
        List<Address> addresses = normalized1.getSortedAddresses();
        Assert.assertEquals("127.0.0.1", addresses.get(0).getHost());
        Assert.assertEquals("127.0.0.2", addresses.get(1).getHost());
        Assert.assertEquals("127.0.0.3", addresses.get(2).getHost());
    }

    @Test
    public void testSameHostDifferentPorts() {
        Endpoints endpoints1 = new Endpoints("127.0.0.1:8082;127.0.0.1:8080;127.0.0.1:8081");
        Endpoints endpoints2 = new Endpoints("127.0.0.1:8080;127.0.0.1:8082;127.0.0.1:8081");
        
        NormalizedEndpoints normalized1 = new NormalizedEndpoints(endpoints1);
        NormalizedEndpoints normalized2 = new NormalizedEndpoints(endpoints2);
        
        Assert.assertEquals(normalized1, normalized2);
        
        // Verify sorted order by port
        List<Address> addresses = normalized1.getSortedAddresses();
        Assert.assertEquals(8080, addresses.get(0).getPort());
        Assert.assertEquals(8081, addresses.get(1).getPort());
        Assert.assertEquals(8082, addresses.get(2).getPort());
    }
}
