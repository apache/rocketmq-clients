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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.misc.Validators;
import org.apache.rocketmq.client.remoting.CredentialsProvider;
import org.apache.rocketmq.utility.UtilAll;

public class ClientConfig {
    private static final String CLIENT_ID_SEPARATOR = "@";

    protected long ioTimeoutMillis = 3 * 1000;

    protected final String clientId;

    protected String group;

    protected String arn = "";

    protected boolean messageTracingEnabled = true;

    protected boolean updateMessageTracerAsync = false;

    // TODO: fix region_id here.
    private String regionId = "cn-hangzhou";

    private String tenantId = "";
    // TODO: fix service name here.
    private String serviceName = "MQ";

    private CredentialsProvider credentialsProvider = null;

    public ClientConfig(String group) {
        this.group = group;

        StringBuilder sb = new StringBuilder();
        final String hostName = UtilAll.hostName();
        sb.append(hostName);
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(UtilAll.processId());
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(Long.toString(System.nanoTime(), 36));
        this.clientId = sb.toString();
    }

    public void setGroup(String group) throws ClientException {
        Validators.checkGroup(group);
        this.group = group;
    }

    public String getGroup() {
        return group;
    }


    // TODO: not allowed to update after client manager started(override in producer and consumer)
    public void setArn(String arn) {
        checkNotNull(arn, "Abstract resource name is null, please set it.");
        this.arn = arn;
    }


    // TODO: not allowed to update after client manager started(override in producer and consumer)
    public void setCredentialsProvider(CredentialsProvider credentialsProvider) {
        checkNotNull(credentialsProvider, "Credentials provider is null, please set it.");
        this.credentialsProvider = credentialsProvider;
    }

    public long getIoTimeoutMillis() {
        return this.ioTimeoutMillis;
    }

    public String getClientId() {
        return this.clientId;
    }

    public String getArn() {
        return this.arn;
    }

    public boolean isMessageTracingEnabled() {
        return this.messageTracingEnabled;
    }

    public boolean isUpdateMessageTracerAsync() {
        return this.updateMessageTracerAsync;
    }

    public String getRegionId() {
        return this.regionId;
    }

    public String getTenantId() {
        return this.tenantId;
    }

    public String getServiceName() {
        return this.serviceName;
    }

    public CredentialsProvider getCredentialsProvider() {
        return this.credentialsProvider;
    }

    public void setIoTimeoutMillis(long ioTimeoutMillis) {
        this.ioTimeoutMillis = ioTimeoutMillis;
    }

    public void setMessageTracingEnabled(boolean messageTracingEnabled) {
        this.messageTracingEnabled = messageTracingEnabled;
    }

    public void setUpdateMessageTracerAsync(boolean updateMessageTracerAsync) {
        this.updateMessageTracerAsync = updateMessageTracerAsync;
    }

    public void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
}
