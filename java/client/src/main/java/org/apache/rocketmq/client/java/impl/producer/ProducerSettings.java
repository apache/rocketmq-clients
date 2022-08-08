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

package org.apache.rocketmq.client.java.impl.producer;

import apache.rocketmq.v2.Publishing;
import apache.rocketmq.v2.Resource;
import apache.rocketmq.v2.Settings;
import com.google.common.base.MoreObjects;
import com.google.common.util.concurrent.Futures;
import com.google.protobuf.util.Durations;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.rocketmq.client.java.impl.ClientSettings;
import org.apache.rocketmq.client.java.impl.ClientType;
import org.apache.rocketmq.client.java.impl.UserAgent;
import org.apache.rocketmq.client.java.retry.ExponentialBackoffRetryPolicy;
import org.apache.rocketmq.client.java.retry.RetryPolicy;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerSettings extends ClientSettings {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerSettings.class);

    private final Set<String> topics;
    /**
     * If message body size exceeds the threshold, it would be compressed for convenience of transport.
     */
    private volatile int maxBodySizeBytes = 4 * 1024 * 1024;
    private volatile boolean validateMessageType = true;

    public ProducerSettings(String clientId, Endpoints accessPoint, ExponentialBackoffRetryPolicy retryPolicy,
        Duration requestTimeout, Set<String> topics) {
        super(clientId, ClientType.PRODUCER, accessPoint, retryPolicy, requestTimeout);
        this.topics = topics;
    }

    public int getMaxBodySizeBytes() {
        return maxBodySizeBytes;
    }

    public boolean isValidateMessageType() {
        return validateMessageType;
    }

    @Override
    public Settings toProtobuf() {
        final Publishing publishing = Publishing.newBuilder()
            .addAllTopics(topics.stream().map(name -> Resource.newBuilder().setName(name).build())
                .collect(Collectors.toList())).setValidateMessageType(validateMessageType).build();
        final Settings.Builder builder = Settings.newBuilder()
            .setAccessPoint(accessPoint.toProtobuf()).setClientType(clientType.toProtobuf())
            .setRequestTimeout(Durations.fromNanos(requestTimeout.toNanos())).setPublishing(publishing);
        return builder.setBackoffPolicy(retryPolicy.toProtobuf()).setUserAgent(UserAgent.INSTANCE.toProtoBuf()).build();
    }

    @Override
    public void applySettingsCommand(Settings settings) {
        final Settings.PubSubCase pubSubCase = settings.getPubSubCase();
        if (!Settings.PubSubCase.PUBLISHING.equals(pubSubCase)) {
            LOGGER.error("[Bug] Issued settings not match with the client type, clientId={}, pubSubCase={}, "
                + "clientType={}", clientId, pubSubCase, clientType);
            return;
        }
        final apache.rocketmq.v2.RetryPolicy backoffPolicy = settings.getBackoffPolicy();
        final Publishing publishing = settings.getPublishing();
        RetryPolicy exist = retryPolicy;
        this.retryPolicy = exist.inheritBackoff(backoffPolicy);
        this.validateMessageType = settings.getPublishing().getValidateMessageType();
        this.maxBodySizeBytes = publishing.getMaxBodySize();
        this.arrivedFuture.setFuture(Futures.immediateVoidFuture());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("clientId", clientId)
            .add("clientType", clientType)
            .add("accessPoint", accessPoint)
            .add("retryPolicy", retryPolicy)
            .add("requestTimeout", requestTimeout)
            .add("topics", topics)
            .add("maxBodySizeBytes", maxBodySizeBytes)
            .toString();
    }
}
