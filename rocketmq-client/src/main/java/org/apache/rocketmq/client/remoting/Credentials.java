package org.apache.rocketmq.client.remoting;

public interface Credentials {
    AccessCredential getAccessCredential();

    String getTenantId();

    String getArn();

    String getRegionId();

    String getServiceName();
}
