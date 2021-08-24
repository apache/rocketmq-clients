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

    public ClientManager registerObserver(String managerId, ClientObserver observer) {
        managersTableLock.lock();
        try {
            ClientManager manager = managersTable.get(managerId);
            if (null == manager) {
                manager = new ClientManagerImpl(managerId);
                manager.start();
                managersTable.put(managerId, manager);
                manager.registerObserver(observer);
            }
            return manager;
        } finally {
            managersTableLock.unlock();
        }
    }

    public void unregisterObserver(String managerId, ClientObserver observer) throws InterruptedException {
        ClientManager removedManager = null;
        managersTableLock.lock();
        try {
            final ClientManager manager = managersTable.get(managerId);
            if (null == manager) {
                return;
            }
            manager.unregisterObserver(observer);
            if (manager.isEmpty()) {
                removedManager = manager;
                managersTable.remove(managerId);
            }
        } finally {
            managersTableLock.unlock();
        }
        if (null != removedManager) {
            removedManager.shutdown();
        }
    }
}
