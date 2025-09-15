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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import javax.net.ssl.SSLException;
import org.apache.rocketmq.client.apis.ClientException;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.apache.rocketmq.client.java.route.NormalizedEndpoints;
import org.apache.rocketmq.client.java.rpc.RpcClient;
import org.apache.rocketmq.client.java.rpc.RpcClientImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Pooled implementation of ClientManager that shares RPC connections across multiple client instances.
 * This implementation maintains a global connection pool to reduce resource usage when multiple
 * clients connect to the same endpoints.
 */
public class PooledClientManagerImpl extends ClientManagerImpl {
    private static final Logger log = LoggerFactory.getLogger(PooledClientManagerImpl.class);

    /**
     * Global shared RPC client pool across all client instances.
     * Key: NormalizedEndpoints (order-independent), Value: PooledRpcClient wrapper
     */
    @GuardedBy("globalRpcClientPoolLock")
    private static final Map<NormalizedEndpoints, PooledRpcClient> globalRpcClientPool = new HashMap<>();
    private static final ReadWriteLock globalRpcClientPoolLock = new ReentrantReadWriteLock();

    /**
     * Local reference to pooled RPC clients used by this client manager instance.
     * This helps track which pooled clients this instance is using for cleanup purposes.
     * Using ConcurrentHashMap for lock-free reads in the fast path.
     * Key: NormalizedEndpoints (order-independent), Value: PooledRpcClient wrapper
     */
    private final ConcurrentHashMap<NormalizedEndpoints, PooledRpcClient> localRpcClientRefs;

    /**
     * Cache for mapping Endpoints to NormalizedEndpoints to avoid repeated sorting.
     */
    private static final ConcurrentMap<Endpoints, NormalizedEndpoints> normalizedEndpointsCache = new ConcurrentHashMap<>();

    public PooledClientManagerImpl(Client client) {
        super(client);
        this.localRpcClientRefs = new ConcurrentHashMap<>();
    }

    /**
     * Wrapper class for RPC client that includes reference counting for connection pooling.
     */
    private static class PooledRpcClient {
        private final RpcClient rpcClient;
        private int referenceCount;
        private volatile long lastAccessTime;

        public PooledRpcClient(RpcClient rpcClient) {
            this.rpcClient = rpcClient;
            this.referenceCount = 1;
            this.lastAccessTime = System.currentTimeMillis();
        }

        public RpcClient getRpcClient() {
            this.lastAccessTime = System.currentTimeMillis();
            return rpcClient;
        }

        public synchronized void addReference() {
            referenceCount++;
            this.lastAccessTime = System.currentTimeMillis();
        }

        public synchronized int removeReference() {
            referenceCount--;
            return referenceCount;
        }

        public synchronized int getReferenceCount() {
            return referenceCount;
        }

        public Duration getIdleDuration() {
            return Duration.ofMillis(System.currentTimeMillis() - lastAccessTime);
        }
    }

    /**
     * Override the getRpcClient method to use connection pooling.
     * This method will reuse existing connections when possible and create new ones only when necessary.
     * Optimized for high-frequency access with minimal lock contention.
     * Uses NormalizedEndpoints to ensure address order independence.
     */
    @Override
    protected RpcClient getRpcClient(Endpoints endpoints) throws ClientException {
        // 优先从缓存获取 NormalizedEndpoints，避免重复排序
        NormalizedEndpoints normalizedEndpoints = normalizedEndpointsCache.computeIfAbsent(
            endpoints, k -> new NormalizedEndpoints(k)
        );

        // Fast path: check local cache without lock (volatile read)
        PooledRpcClient pooledClient = localRpcClientRefs.get(normalizedEndpoints);
        if (pooledClient != null) {
            return pooledClient.getRpcClient();
        }

        // Slow path: need to check global pool or create new connection
        return getRpcClientSlowPath(endpoints, normalizedEndpoints);
    }

    /**
     * Slow path for getRpcClient when local cache miss occurs.
     * This method handles global pool lookup and connection creation.
     */
    private RpcClient getRpcClientSlowPath(Endpoints endpoints, NormalizedEndpoints normalizedEndpoints) 
            throws ClientException {
        // Check global pool first
        globalRpcClientPoolLock.readLock().lock();
        try {
            PooledRpcClient pooledClient = globalRpcClientPool.get(normalizedEndpoints);
            if (pooledClient != null) {
                // Add reference and store local reference
                pooledClient.addReference();
                addLocalReference(normalizedEndpoints, pooledClient);
                return pooledClient.getRpcClient();
            }
        } finally {
            globalRpcClientPoolLock.readLock().unlock();
        }

        // Need to create new connection - upgrade to write lock
        globalRpcClientPoolLock.writeLock().lock();
        try {
            // Double-check after acquiring write lock
            PooledRpcClient pooledClient = globalRpcClientPool.get(normalizedEndpoints);
            if (pooledClient != null) {
                pooledClient.addReference();
                addLocalReference(normalizedEndpoints, pooledClient);
                return pooledClient.getRpcClient();
            }

            // Create new RPC client using original endpoints
            try {
                RpcClient rpcClient = new RpcClientImpl(endpoints, getClient().isSslEnabled());
                pooledClient = new PooledRpcClient(rpcClient);
                globalRpcClientPool.put(normalizedEndpoints, pooledClient);
                addLocalReference(normalizedEndpoints, pooledClient);
                log.info("Created new pooled RPC client, endpoints={}, normalizedEndpoints={}, clientId={}", 
                    endpoints, normalizedEndpoints, getClient().getClientId());
                return pooledClient.getRpcClient();
            } catch (SSLException e) {
                log.error("Failed to create pooled RPC client, endpoints={}, clientId={}", 
                    endpoints, getClient().getClientId(), e);
                throw new ClientException("Failed to generate RPC client", e);
            }
        } finally {
            globalRpcClientPoolLock.writeLock().unlock();
        }
    }

    private void addLocalReference(NormalizedEndpoints normalizedEndpoints, PooledRpcClient pooledClient) {
        localRpcClientRefs.put(normalizedEndpoints, pooledClient);
    }

    /**
     * Override the clearIdleRpcClients method to work with the global pool.
     */
    @Override
    protected void clearIdleRpcClients() throws InterruptedException {
        globalRpcClientPoolLock.writeLock().lock();
        try {
            Iterator<Map.Entry<NormalizedEndpoints, PooledRpcClient>> it = globalRpcClientPool.entrySet().iterator();
            while (it.hasNext()) {
                Map.Entry<NormalizedEndpoints, PooledRpcClient> entry = it.next();
                NormalizedEndpoints normalizedEndpoints = entry.getKey();
                PooledRpcClient pooledClient = entry.getValue();

                Duration idleDuration = pooledClient.getIdleDuration();
                // Only remove if idle for too long AND no references
                if (idleDuration.compareTo(RPC_CLIENT_MAX_IDLE_DURATION) > 0 && 
                    pooledClient.getReferenceCount() == 0) {
                    it.remove();
                    pooledClient.getRpcClient().shutdown();
                    log.info("Removed idle pooled RPC client, normalizedEndpoints={}, idleDuration={}, " +
                        "rpcClientMaxIdleDuration={}", normalizedEndpoints, idleDuration, RPC_CLIENT_MAX_IDLE_DURATION);
                }
            }
        } finally {
            globalRpcClientPoolLock.writeLock().unlock();
        }
    }

    /**
     * Override shutdown to properly clean up references in the global pool.
     */
    @Override
    protected void shutDown() throws IOException {
        log.info("Begin to shutdown the pooled client manager, clientId={}", getClient().getClientId());
        
        // Remove references from global pool
        globalRpcClientPoolLock.writeLock().lock();
        try {
            for (Map.Entry<NormalizedEndpoints, PooledRpcClient> entry : localRpcClientRefs.entrySet()) {
                NormalizedEndpoints normalizedEndpoints = entry.getKey();
                PooledRpcClient pooledClient = entry.getValue();
                
                int remainingRefs = pooledClient.removeReference();
                log.debug("Removed reference for normalizedEndpoints={}, remaining references={}, clientId={}", 
                    normalizedEndpoints, remainingRefs, getClient().getClientId());
                
                // If no more references, remove from global pool and shutdown
                if (remainingRefs == 0) {
                    globalRpcClientPool.remove(normalizedEndpoints);
                    try {
                        pooledClient.getRpcClient().shutdown();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        log.warn("Interrupted while shutting down pooled RPC client, normalizedEndpoints={}, clientId={}", 
                            normalizedEndpoints, getClient().getClientId(), e);
                    }
                    log.info("Shutdown pooled RPC client due to no references, normalizedEndpoints={}, clientId={}", 
                        normalizedEndpoints, getClient().getClientId());
                }
            }
            localRpcClientRefs.clear();
        } finally {
            globalRpcClientPoolLock.writeLock().unlock();
        }

        // Call parent shutdown for other cleanup
        super.shutDown();
        log.info("Shutdown the pooled client manager successfully, clientId={}", getClient().getClientId());
    }

    /**
     * Helper method to access the client instance from parent class.
     */
    private Client getClient() {
        return client;
    }
}
