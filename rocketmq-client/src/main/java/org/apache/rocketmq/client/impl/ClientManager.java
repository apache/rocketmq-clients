package org.apache.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;

public class ClientManager {

  private static final ConcurrentHashMap<String /* Client Id */, ClientInstance>
      clientInstanceTable = new ConcurrentHashMap<String, ClientInstance>();

  private ClientManager() {}

  public static ClientInstance getClientInstance(final ClientConfig clientConfig) {
    final String clientId = clientConfig.buildClientId();
    ClientInstance clientInstance = clientInstanceTable.get(clientId);
    if (null != clientInstance) {
      return clientInstance;
    }
    clientInstance = new ClientInstance(clientConfig, clientId);
    final ClientInstance preClientInstance =
        clientInstanceTable.putIfAbsent(clientId, clientInstance);
    if (null != preClientInstance) {
      clientInstance = preClientInstance;
    }
    return clientInstance;
  }
}
