package org.apache.rocketmq.client.impl;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.remoting.AccessCredential;
import org.apache.rocketmq.client.remoting.Address;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.route.AddressScheme;
import org.apache.rocketmq.utility.RemotingUtil;
import org.apache.rocketmq.utility.UtilAll;

@Getter
@Setter
public class ClientConfig {
    private static final String CLIENT_ID_SEPARATOR = "@";

    private final ClientInstanceConfig clientInstanceConfig;

    private String groupName;

    private final String clientId;

    private Endpoints nameServerEndpoints = null;
    private boolean messageTracingEnabled = false;
    private boolean rpcTracingEnabled = false;

    public ClientConfig(String groupName) {
        this.clientInstanceConfig = new ClientInstanceConfig();
        this.groupName = groupName;

        StringBuilder sb = new StringBuilder();
        final String clientIp = RemotingUtil.getLocalAddress();
        sb.append(clientIp);
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(UtilAll.processId());
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(System.nanoTime());
        this.clientId = sb.toString();
    }

    // TODO: support 127.0.0.1:9876;127.0.0.1:9877 ?
    public void setNamesrvAddr(String namesrv) {
        // TODO: check name server format, IPv4/IPv6/DOMAIN_NAME
        final String[] split = namesrv.split(":");
        String host = split[0];
        int port = Integer.parseInt(split[1]);

        List<Address> addresses = new ArrayList<Address>();
        addresses.add(new Address(host, port));
        this.nameServerEndpoints = new Endpoints(AddressScheme.IPv4, addresses);
    }

    // TODO: not allowed to update after client instance started(override in producer and consumer)
    public void setArn(String arn) {
        clientInstanceConfig.setArn(arn);
    }

    public String getArn() {
        return clientInstanceConfig.getArn();
    }

    // TODO: not allowed to update after client instance started(override in producer and consumer)
    public void setAccessCredential(AccessCredential accessCredential) {
        clientInstanceConfig.setAccessCredential(accessCredential);
    }

    public AccessCredential getAccessCredential() {
        return clientInstanceConfig.getAccessCredential();
    }
}
