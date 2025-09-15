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

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.Collections;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.SessionCredentialsProvider;
import org.apache.rocketmq.client.apis.StaticSessionCredentialsProvider;
import org.apache.rocketmq.client.java.tool.TestBase;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class ClientImplConnectionPoolTest extends TestBase {

    @Test
    public void testClientManagerSelectionWithConnectionPoolDisabled() throws Exception {
        // Create client configuration with connection pool disabled
        ClientConfiguration config = createClientConfiguration(false);
        
        // Create a test client implementation
        TestClientImpl client = new TestClientImpl(config, Collections.singleton(FAKE_TOPIC_0));
        
        try {
            // Get the client manager using reflection
            ClientManager clientManager = getClientManager(client);
            
            // Should be regular ClientManagerImpl, not PooledClientManagerImpl
            Assert.assertTrue("Should use regular ClientManagerImpl when connection pool is disabled",
                clientManager instanceof ClientManagerImpl);
            Assert.assertFalse("Should not use PooledClientManagerImpl when connection pool is disabled",
                clientManager instanceof PooledClientManagerImpl);
        } finally {
            if (client.isRunning()) {
                client.stopAsync().awaitTerminated();
            }
        }
    }

    @Test
    public void testClientManagerSelectionWithConnectionPoolEnabled() throws Exception {
        // Create client configuration with connection pool enabled
        ClientConfiguration config = createClientConfiguration(true);
        
        // Create a test client implementation
        TestClientImpl client = new TestClientImpl(config, Collections.singleton(FAKE_TOPIC_0));
        
        try {
            // Get the client manager using reflection
            ClientManager clientManager = getClientManager(client);
            
            // Should be PooledClientManagerImpl
            Assert.assertTrue("Should use PooledClientManagerImpl when connection pool is enabled",
                clientManager instanceof PooledClientManagerImpl);
        } finally {
            if (client.isRunning()) {
                client.stopAsync().awaitTerminated();
            }
        }
    }

    @Test
    public void testDefaultConnectionPoolSetting() throws Exception {
        // Create client configuration without explicitly setting connection pool
        SessionCredentialsProvider credentialsProvider = 
            new StaticSessionCredentialsProvider("accessKey", "secretKey");
        
        ClientConfiguration config = ClientConfiguration.newBuilder()
            .setEndpoints(FAKE_ENDPOINTS)
            .setCredentialProvider(credentialsProvider)
            .setRequestTimeout(Duration.ofSeconds(3))
            // Note: not calling enableConnectionPool() - should default to false
            .build();
        
        TestClientImpl client = new TestClientImpl(config, Collections.singleton(FAKE_TOPIC_0));
        
        try {
            ClientManager clientManager = getClientManager(client);
            
            // Should default to regular ClientManagerImpl (connection pool disabled by default)
            Assert.assertTrue("Should default to regular ClientManagerImpl",
                clientManager instanceof ClientManagerImpl);
            Assert.assertFalse("Should not default to PooledClientManagerImpl",
                clientManager instanceof PooledClientManagerImpl);
            
            // Verify the configuration
            Assert.assertFalse("Connection pool should be disabled by default",
                config.isConnectionPoolEnabled());
        } finally {
            if (client.isRunning()) {
                client.stopAsync().awaitTerminated();
            }
        }
    }

    @Test
    public void testConnectionPoolConfigurationPersistence() throws Exception {
        // Test that the configuration is correctly stored and retrieved
        ClientConfiguration configDisabled = createClientConfiguration(false);
        ClientConfiguration configEnabled = createClientConfiguration(true);
        
        Assert.assertFalse("Disabled config should return false", 
            configDisabled.isConnectionPoolEnabled());
        Assert.assertTrue("Enabled config should return true", 
            configEnabled.isConnectionPoolEnabled());
        
        // Test multiple calls return consistent results
        Assert.assertFalse(configDisabled.isConnectionPoolEnabled());
        Assert.assertTrue(configEnabled.isConnectionPoolEnabled());
    }

    // Helper methods

    private ClientConfiguration createClientConfiguration(boolean connectionPoolEnabled) {
        SessionCredentialsProvider credentialsProvider = 
            new StaticSessionCredentialsProvider("accessKey", "secretKey");
        
        return ClientConfiguration.newBuilder()
            .setEndpoints(FAKE_ENDPOINTS)
            .setCredentialProvider(credentialsProvider)
            .setRequestTimeout(Duration.ofSeconds(3))
            .enableConnectionPool(connectionPoolEnabled)
            .build();
    }

    private ClientManager getClientManager(ClientImpl client) throws Exception {
        Field field = ClientImpl.class.getDeclaredField("clientManager");
        field.setAccessible(true);
        return (ClientManager) field.get(client);
    }

    /**
     * Test implementation of ClientImpl for testing purposes.
     */
    private static class TestClientImpl extends ClientImpl {
        public TestClientImpl(ClientConfiguration clientConfiguration, java.util.Set<String> topics) {
            super(clientConfiguration, topics);
        }

        @Override
        public Settings getSettings() {
            return Mockito.mock(Settings.class);
        }

        @Override
        public apache.rocketmq.v2.NotifyClientTerminationRequest wrapNotifyClientTerminationRequest() {
            return apache.rocketmq.v2.NotifyClientTerminationRequest.newBuilder().build();
        }

        @Override
        public apache.rocketmq.v2.HeartbeatRequest wrapHeartbeatRequest() {
            return apache.rocketmq.v2.HeartbeatRequest.newBuilder().build();
        }
    }
}