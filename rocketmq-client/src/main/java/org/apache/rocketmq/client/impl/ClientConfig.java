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
import org.apache.rocketmq.client.remoting.Credentials;
import org.apache.rocketmq.client.remoting.CredentialsProvider;
import org.apache.rocketmq.utility.UtilAll;

public class ClientConfig {
    public static final String DEFAULT_REGION_ID = "unknown";

    public static final String DEFAULT_SERVICE_NAME = "RocketMQ";

    private static final String CLIENT_ID_SEPARATOR = "@";

    /**
     * Timeout for underlying communication layer.
     */
    protected long ioTimeoutMillis = 3 * 1000;

    /**
     * Unique identifier for each client.
     */
    protected final String id;

    /**
     * Group name for producer/consumer.
     */
    protected String group;

    /**
     * Abstract resource namespace, same topics or groups in different arn are individual.
     */
    protected String namespace = "";

    /**
     * Switch to enable message tracing or not.
     */
    protected boolean tracingEnabled = true;

    /**
     * If your service is deployed by region, region id could be set here, which would transport with gRPC header.
     */
    private String regionId = DEFAULT_REGION_ID;

    /**
     * Custom service name.
     */
    private String serviceName = DEFAULT_SERVICE_NAME;

    /**
     * If your service is deployed in multi-tenant, tenant id could be set here, which would transport with gRPC header.
     *
     * <p> TODO: parse tenant id from name server address here.
     */
    private String tenantId = "";

    /**
     * Define the provider of {@link Credentials}, refer to implement of {@link Credentials} for more detail.
     */
    private CredentialsProvider credentialsProvider = null;

    public ClientConfig(String group) throws ClientException {
        Validators.checkGroup(group);
        this.group = checkNotNull(group, "group");

        StringBuilder sb = new StringBuilder();
        final String hostName = UtilAll.hostName();
        sb.append(hostName);
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(UtilAll.processId());
        sb.append(CLIENT_ID_SEPARATOR);
        sb.append(Long.toString(System.nanoTime(), 36));
        this.id = sb.toString();
    }

    public void setGroup(String group) throws ClientException {
        Validators.checkGroup(group);
        this.group = checkNotNull(group, "group");
    }

    public String getGroup() {
        return group;
    }

    public void setNamespace(String namespace) {
        this.namespace = checkNotNull(namespace, "namespace");
    }

    public void setCredentialsProvider(CredentialsProvider credentialsProvider) {
        checkNotNull(credentialsProvider, "credentialsProvider");
        this.credentialsProvider = credentialsProvider;
    }

    public long getIoTimeoutMillis() {
        return this.ioTimeoutMillis;
    }

    public String id() {
        return this.id;
    }

    public String getNamespace() {
        return this.namespace;
    }

    public boolean isTracingEnabled() {
        return this.tracingEnabled;
    }

    public String getRegionId() {
        return this.regionId;
    }

    public String getTenantId() {
        return this.tenantId;
    }

    public CredentialsProvider getCredentialsProvider() {
        return this.credentialsProvider;
    }

    public void setIoTimeoutMillis(long ioTimeoutMillis) {
        this.ioTimeoutMillis = ioTimeoutMillis;
    }

    public void setTracingEnabled(boolean tracingEnabled) {
        this.tracingEnabled = tracingEnabled;
    }

    void setRegionId(String regionId) {
        this.regionId = regionId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getServiceName() {
        return serviceName;
    }

    public void setServiceName(String serviceName) {
        this.serviceName = serviceName;
    }
}
