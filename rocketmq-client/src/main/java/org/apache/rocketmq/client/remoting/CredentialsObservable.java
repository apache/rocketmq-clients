package org.apache.rocketmq.client.remoting;

public interface CredentialsObservable {
    AccessCredential getAccessCredential();

    String getTenantId();

    String getArn();

    String getRegionId();

    String getServiceName();
}
