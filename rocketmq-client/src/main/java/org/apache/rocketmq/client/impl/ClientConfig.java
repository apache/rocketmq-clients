package org.apache.rocketmq.client.impl;

import static com.google.common.base.Preconditions.checkNotNull;

import lombok.Getter;
import lombok.Setter;
import org.apache.rocketmq.client.remoting.CredentialsProvider;
import org.apache.rocketmq.utility.RemotingUtil;
import org.apache.rocketmq.utility.UtilAll;

public class ClientConfig {
    private static final String CLIENT_ID_SEPARATOR = "@";

    protected long ioTimeoutMillis = 3 * 1000;

    @Getter
    protected final String clientId;

    protected String group = "";

    @Getter
    protected String arn = "";

    @Getter
    @Setter
    protected boolean messageTracingEnabled = true;

    // TODO: fix region_id here.
    @Getter
    private String regionId = "cn-hangzhou";
    @Getter
    private String tenantId = "";
    // TODO: fix service name here.
    @Setter
    @Getter
    private String serviceName = "aone";

    @Getter
    private CredentialsProvider credentialsProvider = null;

    public ClientConfig(String group) {
        this.group = group;

        StringBuilder sb = new StringBuilder();
        final String clientIp = RemotingUtil.getLocalAddress();
        sb.append(clientIp);
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(UtilAll.processId());
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(System.nanoTime());
        this.clientId = sb.toString();
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getGroup() {
        return group;
    }


    // TODO: not allowed to update after client instance started(override in producer and consumer)
    public void setArn(String arn) {
        checkNotNull(arn, "Abstract resource name is null, please set it.");
        this.arn = arn;
    }


    // TODO: not allowed to update after client instance started(override in producer and consumer)
    public void setCredentialsProvider(CredentialsProvider credentialsProvider) {
        checkNotNull(credentialsProvider, "Credentials provider is null, please set it.");
        this.credentialsProvider = credentialsProvider;
    }
}
