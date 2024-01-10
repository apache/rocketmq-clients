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

package org.apache.rocketmq.client.apis;

import java.time.Duration;
import java.util.Optional;

/**
 * Common client configuration.
 */
public class ClientConfiguration {
    private final String endpoints;
    private final SessionCredentialsProvider sessionCredentialsProvider;
    private final Duration requestTimeout;
    private final boolean sslEnabled;
    private final String namespace;

    /**
     * The caller is supposed to have validated the arguments and handled throwing exceptions or
     * logging warnings already, so we avoid repeating args check here.
     */
    ClientConfiguration(String endpoints, SessionCredentialsProvider sessionCredentialsProvider,
        Duration requestTimeout, boolean sslEnabled, String namespace) {
        this.endpoints = endpoints;
        this.sessionCredentialsProvider = sessionCredentialsProvider;
        this.requestTimeout = requestTimeout;
        this.sslEnabled = sslEnabled;
        this.namespace = namespace;
    }

    public static ClientConfigurationBuilder newBuilder() {
        return new ClientConfigurationBuilder();
    }

    public String getEndpoints() {
        return endpoints;
    }

    public Optional<SessionCredentialsProvider> getCredentialsProvider() {
        return Optional.ofNullable(sessionCredentialsProvider);
    }

    public Duration getRequestTimeout() {
        return requestTimeout;
    }

    public boolean isSslEnabled() {
        return sslEnabled;
    }

    public String getNamespace() {
        return namespace;
    }
}
