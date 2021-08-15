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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ClientManagerFactory {

    private static final ClientManagerFactory INSTANCE = new ClientManagerFactory();

    private final Map<String, ClientManager> managersTable;
    private final Lock lock;

    private ClientManagerFactory() {
        this.managersTable = new HashMap<String, ClientManager>();
        this.lock = new ReentrantLock();
    }

    public static ClientManagerFactory getInstance() {
        return INSTANCE;
    }

    public ClientManager getClientManager(final ClientConfig clientConfig) {
        final String arn = clientConfig.getArn();
        lock.lock();
        try {
            ClientManager clientManager = managersTable.get(arn);
            if (null == clientManager) {
                clientManager = new ClientManagerImpl(arn);
                clientManager.start();
                managersTable.put(arn, clientManager);
            }
            return clientManager;
        } finally {
            lock.unlock();
        }
    }

    public void removeClientManager(final String id) {
        lock.lock();
        try {
            managersTable.remove(id);
        } finally {
            lock.unlock();
        }
    }
}
