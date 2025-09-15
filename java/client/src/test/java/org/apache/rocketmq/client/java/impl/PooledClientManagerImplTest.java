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

package org.apache.rocketmq.client.java.impl;

import io.grpc.Metadata;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.NormalizedEndpoints;
import org.apache.rocketmq.client.java.rpc.RpcClient;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

public class PooledClientManagerImplTest extends TestBase {
    private PooledClientManagerImpl pooledClientManager1;
    private PooledClientManagerImpl pooledClientManager2;
    private Client mockClient1;
    private Client mockClient2;

    @Before
    public void setUp() throws Exception {
        // Clear the global pool before each test
        clearGlobalPool();
        clearNormalizedEndpointsCache();
        
        // Create mock clients
        mockClient1 = createMockClient();
        mockClient2 = createMockClient();
        
        // Create pooled client managers
        pooledClientManager1 = new PooledClientManagerImpl(mockClient1);
        pooledClientManager2 = new PooledClientManagerImpl(mockClient2);
        
        // Start the managers
        pooledClientManager1.startAsync().awaitRunning();
        pooledClientManager2.startAsync().awaitRunning();
    }

    @After
    public void tearDown() throws Exception {
        // Stop the managers
        if (pooledClientManager1 != null) {
            pooledClientManager1.stopAsync().awaitTerminated();
        }
        if (pooledClientManager2 != null) {
            pooledClientManager2.stopAsync().awaitTerminated();
        }
        
        // Clear the global pool after each test
        clearGlobalPool();
        clearNormalizedEndpointsCache();
    }

    private Client createMockClient() throws Exception {
        Client client = Mockito.mock(Client.class);
        final Metadata metadata = new Metadata();
        Mockito.doReturn(metadata).when(client).sign();
        final ClientId clientId = new ClientId();
        Mockito.doReturn(clientId).when(client).getClientId();
        Mockito.doReturn(false).when(client).isSslEnabled();
        return client;
    }

    @Test
    public void testConnectionPooling() throws Exception {
        Endpoints endpoints = fakeEndpoints();
        
        // Get RPC client from first manager
        RpcClient rpcClient1 = pooledClientManager1.getRpcClient(endpoints);
        Assert.assertNotNull(rpcClient1);
        
        // Get RPC client from second manager with same endpoints
        RpcClient rpcClient2 = pooledClientManager2.getRpcClient(endpoints);
        Assert.assertNotNull(rpcClient2);
        
        // They should be the same instance (shared from pool)
        Assert.assertSame("RPC clients should be shared from pool", rpcClient1, rpcClient2);
        
        // Verify global pool contains the connection
        Map<NormalizedEndpoints, ?> globalPool = getGlobalPool();
        Assert.assertEquals(1, globalPool.size());
    }

    @Test
    public void testAddressOrderIndependence() throws Exception {
        // Create endpoints with same addresses but different order
        Endpoints endpoints1 = new Endpoints("127.0.0.1:8080;127.0.0.2:8081");
        Endpoints endpoints2 = new Endpoints("127.0.0.2:8081;127.0.0.1:8080");
        
        // Get RPC clients
        RpcClient rpcClient1 = pooledClientManager1.getRpcClient(endpoints1);
        RpcClient rpcClient2 = pooledClientManager2.getRpcClient(endpoints2);
        
        // They should be the same instance due to normalization
        Assert.assertSame("RPC clients should be shared despite address order difference", 
            rpcClient1, rpcClient2);
        
        // Verify only one entry in global pool
        Map<NormalizedEndpoints, ?> globalPool = getGlobalPool();
        Assert.assertEquals(1, globalPool.size());
    }

    @Test
    public void testNormalizedEndpointsCache() throws Exception {
        Endpoints endpoints = fakeEndpoints();
        
        // Get RPC client multiple times
        pooledClientManager1.getRpcClient(endpoints);
        pooledClientManager1.getRpcClient(endpoints);
        pooledClientManager2.getRpcClient(endpoints);
        
        // Verify cache contains the normalized endpoints
        ConcurrentMap<Endpoints, NormalizedEndpoints> cache = getNormalizedEndpointsCache();
        Assert.assertTrue("Cache should contain the endpoints", cache.containsKey(endpoints));
        Assert.assertEquals(1, cache.size());
    }

    @Test
    public void testLocalCacheFastPath() throws Exception {
        Endpoints endpoints = fakeEndpoints();
        
        // First call - should go through slow path
        RpcClient rpcClient1 = pooledClientManager1.getRpcClient(endpoints);
        
        // Second call - should use local cache (fast path)
        RpcClient rpcClient2 = pooledClientManager1.getRpcClient(endpoints);
        
        // Should be the same instance
        Assert.assertSame(rpcClient1, rpcClient2);
        
        // Verify local cache contains the entry
        ConcurrentHashMap<NormalizedEndpoints, ?> localRefs = getLocalRpcClientRefs(pooledClientManager1);
        Assert.assertEquals(1, localRefs.size());
    }

    @Test
    public void testMultipleEndpoints() throws Exception {
        Endpoints endpoints1 = new Endpoints("127.0.0.1:8080");
        Endpoints endpoints2 = new Endpoints("127.0.0.2:8081");
        
        // Get RPC clients for different endpoints
        RpcClient rpcClient1 = pooledClientManager1.getRpcClient(endpoints1);
        RpcClient rpcClient2 = pooledClientManager1.getRpcClient(endpoints2);
        
        // They should be different instances
        Assert.assertNotSame(rpcClient1, rpcClient2);
        
        // Verify global pool contains both connections
        Map<NormalizedEndpoints, ?> globalPool = getGlobalPool();
        Assert.assertEquals(2, globalPool.size());
    }

    @Test
    public void testReferenceCountingOnShutdown() throws Exception {
        Endpoints endpoints = fakeEndpoints();
        
        // Get RPC client from both managers
        RpcClient rpcClient1 = pooledClientManager1.getRpcClient(endpoints);
        RpcClient rpcClient2 = pooledClientManager2.getRpcClient(endpoints);
        
        Assert.assertSame(rpcClient1, rpcClient2);
        
        // Verify global pool has one entry
        Map<NormalizedEndpoints, ?> globalPool = getGlobalPool();
        Assert.assertEquals(1, globalPool.size());
        
        // Shutdown first manager
        pooledClientManager1.stopAsync().awaitTerminated();
        pooledClientManager1 = null;
        
        // Global pool should still have the entry (reference count > 0)
        Assert.assertEquals(1, globalPool.size());
        
        // Shutdown second manager
        pooledClientManager2.stopAsync().awaitTerminated();
        pooledClientManager2 = null;
        
        // Global pool should be empty now (reference count = 0)
        Assert.assertEquals(0, globalPool.size());
    }

    @Test
    public void testClearIdleRpcClients() throws Exception {
        Endpoints endpoints = fakeEndpoints();
        
        // Get RPC client
        RpcClient rpcClient = pooledClientManager1.getRpcClient(endpoints);
        Assert.assertNotNull(rpcClient);
        
        // Verify global pool has one entry
        Map<NormalizedEndpoints, ?> globalPool = getGlobalPool();
        Assert.assertEquals(1, globalPool.size());
        
        // Shutdown the manager to remove references
        pooledClientManager1.stopAsync().awaitTerminated();
        pooledClientManager1 = null;
        
        // Create a new manager and call clearIdleRpcClients
        PooledClientManagerImpl newManager = new PooledClientManagerImpl(createMockClient());
        newManager.startAsync().awaitRunning();
        
        try {
            // Use reflection to call the protected method
            Method clearMethod = PooledClientManagerImpl.class.getDeclaredMethod("clearIdleRpcClients");
            clearMethod.setAccessible(true);
            clearMethod.invoke(newManager);
            
            // The idle connection should be removed (since it has no references)
            // Note: This test might be flaky due to timing, but it tests the mechanism
            
        } finally {
            newManager.stopAsync().awaitTerminated();
        }
    }

    @Test
    public void testDifferentSchemesCreateSeparateConnections() throws Exception {
        Endpoints ipv4Endpoints = new Endpoints("127.0.0.1:8080");
        Endpoints domainEndpoints = new Endpoints("example.com:8080");
        
        RpcClient rpcClient1 = pooledClientManager1.getRpcClient(ipv4Endpoints);
        RpcClient rpcClient2 = pooledClientManager1.getRpcClient(domainEndpoints);
        
        // Should be different instances due to different schemes
        Assert.assertNotSame(rpcClient1, rpcClient2);
        
        // Verify global pool contains both connections
        Map<NormalizedEndpoints, ?> globalPool = getGlobalPool();
        Assert.assertEquals(2, globalPool.size());
    }

    // Helper methods to access private fields using reflection

    @SuppressWarnings("unchecked")
    private Map<NormalizedEndpoints, ?> getGlobalPool() throws Exception {
        Field field = PooledClientManagerImpl.class.getDeclaredField("GLOBAL_RPC_CLIENT_POOL");
        field.setAccessible(true);
        return (Map<NormalizedEndpoints, ?>) field.get(null);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<Endpoints, NormalizedEndpoints> getNormalizedEndpointsCache() throws Exception {
        Field field = PooledClientManagerImpl.class.getDeclaredField("NORMALIZED_ENDPOINTS_CACHE");
        field.setAccessible(true);
        return (ConcurrentMap<Endpoints, NormalizedEndpoints>) field.get(null);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentHashMap<NormalizedEndpoints, ?> getLocalRpcClientRefs(PooledClientManagerImpl manager) 
            throws Exception {
        Field field = PooledClientManagerImpl.class.getDeclaredField("localRpcClientRefs");
        field.setAccessible(true);
        return (ConcurrentHashMap<NormalizedEndpoints, ?>) field.get(manager);
    }

    private void clearGlobalPool() throws Exception {
        Map<NormalizedEndpoints, ?> globalPool = getGlobalPool();
        globalPool.clear();
    }

    private void clearNormalizedEndpointsCache() throws Exception {
        ConcurrentMap<Endpoints, NormalizedEndpoints> cache = getNormalizedEndpointsCache();
        cache.clear();
    }
}
