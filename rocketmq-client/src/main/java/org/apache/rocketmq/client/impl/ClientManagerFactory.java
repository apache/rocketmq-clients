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

@ThreadSafe
public class ClientManagerFactory {

    private static final ClientManagerFactory INSTANCE = new ClientManagerFactory();

    @GuardedBy("managersTableLock")
    private final Map<String, ClientManager> managersTable;
    private final Lock managersTableLock;

    private ClientManagerFactory() {
        this.managersTable = new HashMap<String, ClientManager>();
        this.managersTableLock = new ReentrantLock();
    }

    public static ClientManagerFactory getInstance() {
        return INSTANCE;
    }

    /**
     * Register {@link ClientObserver} to the appointed manager by manager id, start the manager if it is created newly.
     *
     * <p>Different observer would share the same {@link ClientManager} if they has the same manager id.
     *
     * @param managerId client manager id.
     * @param observer  client observer.
     * @return the client manager which is started.
     */
    public ClientManager registerObserver(String managerId, ClientObserver observer) {
        managersTableLock.lock();
        try {
            ClientManager manager = managersTable.get(managerId);
            if (null == manager) {
                // create and start manager.
                manager = new ClientManagerImpl(managerId);
                manager.start();
                managersTable.put(managerId, manager);
            }
            manager.registerObserver(observer);
            return manager;
        } finally {
            managersTableLock.unlock();
        }
    }

    /**
     * Unregister {@link ClientObserver} to the appointed manager by message id, shutdown the manager if no observer
     * registered in it.
     *
     * @param managerId client manager id.
     * @param observer  client observer.
     * @throws InterruptedException if thread has been interrupted.
     */
    public void unregisterObserver(String managerId, ClientObserver observer) throws InterruptedException {
        ClientManager removedManager = null;
        managersTableLock.lock();
        try {
            final ClientManager manager = managersTable.get(managerId);
            if (null == manager) {
                return;
            }
            manager.unregisterObserver(observer);
            // shutdown the manager if no observer registered.
            if (manager.isEmpty()) {
                removedManager = manager;
                managersTable.remove(managerId);
            }
        } finally {
            managersTableLock.unlock();
        }
        // no need to hold the lock here.
        if (null != removedManager) {
            removedManager.shutdown();
        }
    }
}
