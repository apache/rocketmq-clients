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

import com.google.errorprone.annotations.concurrent.GuardedBy;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.ThreadSafe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ThreadSafe
@SuppressWarnings("UnstableApiUsage")
public class ClientManagerFactory {
    private static final Logger log = LoggerFactory.getLogger(ClientManagerFactory.class);

    private static final ClientManagerFactory INSTANCE = new ClientManagerFactory();

    @GuardedBy("managersTableLock")
    private final Map<String, ClientManagerImpl> managersTable;
    private final Lock managersTableLock;

    private ClientManagerFactory() {
        this.managersTable = new HashMap<String, ClientManagerImpl>();
        this.managersTableLock = new ReentrantLock();
    }

    public static ClientManagerFactory getInstance() {
        return INSTANCE;
    }

    /**
     * Register {@link Client} to the appointed manager by manager id, start the manager if it is created newly.
     *
     * <p>Different client would share the same {@link ClientManager} if they have the same manager id.
     *
     * @param managerId client manager id.
     * @param client    client to register.
     * @return the client manager which is started.
     */
    public ClientManager registerClient(String managerId, Client client) {
        managersTableLock.lock();
        try {
            ClientManagerImpl manager = managersTable.get(managerId);
            if (null == manager) {
                // create and start manager.
                manager = new ClientManagerImpl(managerId);
                manager.startAsync().awaitRunning();
                managersTable.put(managerId, manager);
            }
            manager.registerClient(client);
            return manager;
        } finally {
            managersTableLock.unlock();
        }
    }

    /**
     * Unregister {@link Client} to the appointed manager by message id, shutdown the manager if no client
     * registered in it.
     *
     * @param managerId identification of client manager.
     * @param client    client to unregister.
     * @return {@link ClientManager} is removed or not.
     */
    public boolean unregisterClient(String managerId, Client client) {
        ClientManagerImpl removedManager = null;
        managersTableLock.lock();
        try {
            final ClientManagerImpl manager = managersTable.get(managerId);
            if (null == manager) {
                // should never reach here.
                log.error("[Bug] manager not found by managerId={}", managerId);
                return false;
            }
            manager.unregisterClient(client);
            // shutdown the manager if no client registered.
            if (manager.isEmpty()) {
                removedManager = manager;
                managersTable.remove(managerId);
            }
        } finally {
            managersTableLock.unlock();
        }
        // no need to hold the lock here.
        if (null != removedManager) {
            removedManager.stopAsync().awaitTerminated();
        }
        return null != removedManager;
    }
}
