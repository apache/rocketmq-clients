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
import java.lang.reflect.Modifier;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.rpc.RpcClient;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/**
 * Test class to verify that ClientManagerImpl properly exposes protected members
 * for inheritance by PooledClientManagerImpl.
 */
public class ClientManagerImplInheritanceTest extends TestBase {
    
    private TestableClientManagerImpl clientManager;
    private Client mockClient;

    @Before
    public void setUp() throws Exception {
        mockClient = createMockClient();
        clientManager = new TestableClientManagerImpl(mockClient);
        clientManager.startAsync().awaitRunning();
    }

    @After
    public void tearDown() {
        if (clientManager != null) {
            clientManager.stopAsync().awaitTerminated();
        }
    }

    @Test
    public void testClientFieldIsProtected() throws Exception {
        Field clientField = ClientManagerImpl.class.getDeclaredField("client");
        Assert.assertTrue("client field should be protected", 
            Modifier.isProtected(clientField.getModifiers()));
        Assert.assertFalse("client field should not be private", 
            Modifier.isPrivate(clientField.getModifiers()));
    }

    @Test
    public void testGetRpcClientMethodIsProtected() throws Exception {
        Method getRpcClientMethod = ClientManagerImpl.class.getDeclaredMethod("getRpcClient", Endpoints.class);
        Assert.assertTrue("getRpcClient method should be protected", 
            Modifier.isProtected(getRpcClientMethod.getModifiers()));
        Assert.assertFalse("getRpcClient method should not be private", 
            Modifier.isPrivate(getRpcClientMethod.getModifiers()));
    }

    @Test
    public void testClearIdleRpcClientsMethodIsProtected() throws Exception {
        Method clearIdleRpcClientsMethod = ClientManagerImpl.class.getDeclaredMethod("clearIdleRpcClients");
        Assert.assertTrue("clearIdleRpcClients method should be protected", 
            Modifier.isProtected(clearIdleRpcClientsMethod.getModifiers()));
        Assert.assertFalse("clearIdleRpcClients method should not be private", 
            Modifier.isPrivate(clearIdleRpcClientsMethod.getModifiers()));
    }

    @Test
    public void testSubclassCanAccessProtectedClient() {
        // Test that subclass can access the protected client field
        Client accessedClient = clientManager.getProtectedClient();
        Assert.assertSame("Subclass should be able to access protected client field", 
            mockClient, accessedClient);
    }

    @Test
    public void testSubclassCanCallProtectedGetRpcClient() throws Exception {
        Endpoints endpoints = fakeEndpoints();
        
        // Test that subclass can call the protected getRpcClient method
        RpcClient rpcClient = clientManager.callProtectedGetRpcClient(endpoints);
        Assert.assertNotNull("Subclass should be able to call protected getRpcClient method", rpcClient);
    }

    @Test
    public void testSubclassCanCallProtectedClearIdleRpcClients() throws Exception {
        // Test that subclass can call the protected clearIdleRpcClients method
        // This should not throw an exception
        clientManager.callProtectedClearIdleRpcClients();
    }

    @Test
    public void testSubclassCanOverrideProtectedMethods() throws Exception {
        // 主动调用被覆盖的方法，确保标志被设置
        Endpoints endpoints = fakeEndpoints();
        clientManager.callProtectedGetRpcClient(endpoints);
        clientManager.callProtectedClearIdleRpcClients();

        Assert.assertFalse("Test subclass should not have overridden getRpcClient",
            clientManager.isGetRpcClientOverridden());
        Assert.assertFalse("Test subclass should not have overridden clearIdleRpcClients",
            clientManager.isClearIdleRpcClientsOverridden());

        clientManager.getRpcClient(endpoints);
        clientManager.clearIdleRpcClients();

        Assert.assertTrue("Test subclass should have overridden getRpcClient",
            clientManager.isGetRpcClientOverridden());
        Assert.assertTrue("Test subclass should have overridden clearIdleRpcClients",
            clientManager.isClearIdleRpcClientsOverridden());
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

    /**
     * Test subclass of ClientManagerImpl to verify protected member access.
     */
    private static class TestableClientManagerImpl extends ClientManagerImpl {
        private boolean getRpcClientOverridden = false;
        private boolean clearIdleRpcClientsOverridden = false;

        public TestableClientManagerImpl(Client client) {
            super(client);
        }

        // Test method to access protected client field
        public Client getProtectedClient() {
            return this.client;
        }

        // Test method to call protected getRpcClient method
        public RpcClient callProtectedGetRpcClient(Endpoints endpoints) throws ClientException {
            return super.getRpcClient(endpoints);
        }

        // Test method to call protected clearIdleRpcClients method
        public void callProtectedClearIdleRpcClients() throws InterruptedException {
            super.clearIdleRpcClients();
        }

        // Override protected methods to verify they can be overridden
        @Override
        protected RpcClient getRpcClient(Endpoints endpoints) throws ClientException {
            getRpcClientOverridden = true;
            return super.getRpcClient(endpoints);
        }

        @Override
        protected void clearIdleRpcClients() throws InterruptedException {
            clearIdleRpcClientsOverridden = true;
            super.clearIdleRpcClients();
        }

        public boolean isGetRpcClientOverridden() {
            return getRpcClientOverridden;
        }

        public boolean isClearIdleRpcClientsOverridden() {
            return clearIdleRpcClientsOverridden;
        }
    }
}