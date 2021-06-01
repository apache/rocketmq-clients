package org.apache.rocketmq.client.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;

public class ClientInstanceManager {

    private static final ClientInstanceManager CLIENT_INSTANCE_MANAGER = new ClientInstanceManager();

    @GuardedBy("lock")
    private final Map<ClientInstanceConfig, ClientInstance> clientInstanceTable;
    private final Lock lock;

    private ClientInstanceManager() {
        this.clientInstanceTable = new HashMap<ClientInstanceConfig, ClientInstance>();
        this.lock = new ReentrantLock();
    }

    public static ClientInstanceManager getInstance() {
        return CLIENT_INSTANCE_MANAGER;
    }

    public ClientInstance getClientInstance(final ClientConfig clientConfig) {
        try {
            lock.lock();
            final ClientInstanceConfig clientInstanceConfig = clientConfig.getClientInstanceConfig();
            ClientInstance clientInstance = clientInstanceTable.get(clientInstanceConfig);
            if (null != clientInstance) {
                return clientInstance;
            }
            clientInstance = new ClientInstance(clientInstanceConfig, clientConfig.getEndpoints());
            clientInstanceTable.put(clientInstanceConfig, clientInstance);
            return clientInstance;
        } finally {
            lock.unlock();
        }
    }
}
