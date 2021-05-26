package org.apache.rocketmq.client.impl;

import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import org.apache.rocketmq.client.remoting.AccessCredential;
import org.apache.rocketmq.utility.RemotingUtil;
import org.apache.rocketmq.utility.UtilAll;

@Data
public class ClientConfig {
    private final ClientInstanceConfig clientInstanceConfig;

    private String groupName;
    private List<String> nameServerList;
    private final String clientId;

    public ClientConfig(String groupName) {
        this.clientInstanceConfig = new ClientInstanceConfig();
        this.groupName = groupName;
        this.nameServerList = new ArrayList<String>();

        StringBuilder sb = new StringBuilder();
        final String clientIP = RemotingUtil.getLocalAddress();
        sb.append(clientIP);
        sb.append("@");
        sb.append(UtilAll.processId());
        sb.append("@");
        sb.append(System.nanoTime());
        this.clientId = sb.toString();
    }

    public void setNamesrvAddr(String namesrv) {
        this.nameServerList.clear();
        this.nameServerList.add(namesrv);
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
