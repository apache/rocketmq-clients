package org.apache.rocketmq.client.impl;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.apache.rocketmq.client.remoting.AccessCredential;
import org.apache.rocketmq.client.remoting.Address;
import org.apache.rocketmq.client.remoting.Endpoints;
import org.apache.rocketmq.client.route.Schema;
import org.apache.rocketmq.utility.RemotingUtil;
import org.apache.rocketmq.utility.UtilAll;

@Data
public class ClientConfig {
    private final ClientInstanceConfig clientInstanceConfig;

    private String groupName;
    private Endpoints endpoints;

    private final String clientId;

    public ClientConfig(String groupName) {
        this.clientInstanceConfig = new ClientInstanceConfig();
        this.groupName = groupName;
        this.endpoints = null;

        StringBuilder sb = new StringBuilder();
        final String clientIP = RemotingUtil.getLocalAddress();
        sb.append(clientIP);
        sb.append("@");
        sb.append(UtilAll.processId());
        sb.append("@");
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
        this.endpoints = new Endpoints(Schema.IPv4, addresses);
    }

    public void setArn(String arn) {
        clientInstanceConfig.setArn(arn);
    }

    public String getArn() {
        return clientInstanceConfig.getArn();
    }

    public void setAccessCredential(AccessCredential accessCredential) {
        clientInstanceConfig.setAccessCredential(accessCredential);
    }

    public AccessCredential getAccessCredential() {
        return clientInstanceConfig.getAccessCredential();
    }
}
