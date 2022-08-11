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

package org.apache.rocketmq.client.java.impl;

import com.google.common.base.MoreObjects;
import java.time.Duration;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.apache.rocketmq.client.java.route.Endpoints;

public abstract class Settings {
    protected final String clientId;
    protected final ClientType clientType;
    protected final Endpoints accessPoint;
    protected volatile RetryPolicy retryPolicy;
    protected final Duration requestTimeout;

    public Settings(String clientId, ClientType clientType, Endpoints accessPoint, RetryPolicy retryPolicy,
        Duration requestTimeout) {
        this.clientId = clientId;
        this.clientType = clientType;
        this.accessPoint = accessPoint;
        this.retryPolicy = retryPolicy;
        this.requestTimeout = requestTimeout;
    }

    public Settings(String clientId, ClientType clientType, Endpoints accessPoint, Duration requestTimeout) {
        this(clientId, clientType, accessPoint, null, requestTimeout);
    }

    public abstract apache.rocketmq.v2.Settings toProtobuf();

    public abstract void sync(apache.rocketmq.v2.Settings settings);

    public RetryPolicy getRetryPolicy() {
        return retryPolicy;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("clientId", clientId)
            .add("clientType", clientType)
            .add("accessPoint", accessPoint)
            .add("retryPolicy", retryPolicy)
            .add("requestTimeout", requestTimeout)
            .toString();
    }
}
