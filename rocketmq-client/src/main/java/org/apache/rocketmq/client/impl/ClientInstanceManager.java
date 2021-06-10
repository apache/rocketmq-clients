package org.apache.rocketmq.client.impl;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.GuardedBy;

public class ClientInstanceManager {

    private static final ClientInstanceManager CLIENT_INSTANCE_MANAGER = new ClientInstanceManager();

    @GuardedBy("lock")
    private final Map<String/* ClientId */, ClientInstance> clientInstanceTable;
    private final Lock lock;

    private ClientInstanceManager() {
        this.clientInstanceTable = new HashMap<String, ClientInstance>();
        this.lock = new ReentrantLock();
    }

    public static ClientInstanceManager getInstance() {
        return CLIENT_INSTANCE_MANAGER;
    }

    public ClientInstance getClientInstance(final ClientConfig clientConfig) {
        lock.lock();
        try {
            final String clientId = clientConfig.getClientId();
            ClientInstance clientInstance = clientInstanceTable.get(clientId);
            if (null != clientInstance) {
                return clientInstance;
            }
            clientInstance = new ClientInstance(clientConfig, clientConfig.getNameServerEndpoints());
            clientInstanceTable.put(clientId, clientInstance);
            return clientInstance;
        } finally {
            lock.unlock();
        }
    }
}
