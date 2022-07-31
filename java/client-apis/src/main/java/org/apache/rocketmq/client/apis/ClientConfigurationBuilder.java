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

import static com.google.common.base.Preconditions.checkNotNull;

import java.time.Duration;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumer;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumerBuilder;

/**
 * Builder to set {@link ClientConfiguration}.
 */
public class ClientConfigurationBuilder {
    private String endpoints;
    private SessionCredentialsProvider sessionCredentialsProvider = null;
    private Duration requestTimeout = Duration.ofSeconds(3);

    /**
     * Configure the access point with which the SDK should communicate.
     *
     * @param endpoints address of service.
     * @return the client configuration builder instance.
     */
    public ClientConfigurationBuilder setEndpoints(String endpoints) {
        checkNotNull(endpoints, "endpoints should not be not null");
        this.endpoints = endpoints;
        return this;
    }

    /**
     * Config the session credential provider.
     *
     * @param sessionCredentialsProvider session credential provider.
     * @return the client configuration builder instance.
     */
    public ClientConfigurationBuilder setCredentialProvider(SessionCredentialsProvider sessionCredentialsProvider) {
        this.sessionCredentialsProvider = checkNotNull(sessionCredentialsProvider, "credentialsProvider should not " +
            "be null");
        return this;
    }

    /**
     * Configure request timeout for ordinary RPC.
     *
     * <p>request timeout is 3s by default. Especially, the RPC request timeout for long-polling of
     * {@link SimpleConsumer} is increased by request timeout here based on the
     * {@linkplain SimpleConsumerBuilder#setAwaitDuration(Duration) await duration}.
     *
     * @param requestTimeout RPC request timeout.
     * @return the client configuration builder instance.
     */
    public ClientConfigurationBuilder setRequestTimeout(Duration requestTimeout) {
        this.requestTimeout = checkNotNull(requestTimeout, "requestTimeout should not be not null");
        return this;
    }

    /**
     * Finalize the build of {@link ClientConfiguration}.
     *
     * @return the client configuration builder instance.
     */
    public ClientConfiguration build() {
        checkNotNull(endpoints, "endpoints should not be null");
        checkNotNull(requestTimeout, "requestTimeout should not be null");
        return new ClientConfiguration(endpoints, sessionCredentialsProvider, requestTimeout);
    }
}
