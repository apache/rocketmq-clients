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

package org.apache.rocketmq.client.java.impl.consumer;

import apache.rocketmq.v2.Code;
import com.google.common.base.MoreObjects;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.client.apis.ClientConfiguration;
import org.apache.rocketmq.client.apis.consumer.FilterExpression;
import org.apache.rocketmq.client.java.exception.LiteSubscriptionQuotaExceededException;
import org.apache.rocketmq.client.java.impl.ClientType;
import org.apache.rocketmq.client.java.message.protocol.Resource;
import org.apache.rocketmq.client.java.misc.ClientId;
import org.apache.rocketmq.client.java.misc.ExcludeFromJacocoGeneratedReport;
import org.apache.rocketmq.client.java.route.Endpoints;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LitePushConsumerSettings extends PushSubscriptionSettings {
    private static final Logger log = LoggerFactory.getLogger(LitePushConsumerSettings.class);
    // bindTopic for lite push consumer
    final Resource bindTopic;
    private final Set<String> liteTopicSet = ConcurrentHashMap.newKeySet();
    // todo sync from remote
    private final int liteTopicMax = 10000; // Assuming a maximum number of lite topics
    private final AtomicLong version = new AtomicLong(System.currentTimeMillis());

    public LitePushConsumerSettings(ClientConfiguration configuration, ClientId clientId, Endpoints endpoints,
        String bindTopic, String group) {
        // to keep compatibility, lite push consumer subscribe ALL
        super(configuration, clientId, ClientType.LITE_PUSH_CONSUMER, endpoints, group,
            Collections.singletonMap(bindTopic, FilterExpression.SUB_ALL));
        this.bindTopic = new Resource(namespace, bindTopic);
        // lite push consumer is fifo consumer
        this.fifo = true;
    }

    public boolean containsLiteTopic(String liteTopic) {
        return liteTopicSet.contains(liteTopic);
    }

    public boolean addLiteTopic(String liteTopic) throws LiteSubscriptionQuotaExceededException {
        if (liteTopicSet.contains(liteTopic)) {
            return true;
        }
        if (liteTopicSet.size() >= liteTopicMax) {
            throw new LiteSubscriptionQuotaExceededException(
                Code.LITE_SUBSCRIPTION_QUOTA_EXCEEDED_VALUE, null, "Lite subscription quota exceeded " + liteTopicMax);
        }
        liteTopicSet.add(liteTopic);
        version.set(System.currentTimeMillis());
        return true;
    }

    public boolean removeLiteTopic(String liteTopic) {
        if (liteTopicSet.remove(liteTopic)) {
            version.set(System.currentTimeMillis());
            return true;
        }
        return false;
    }

    public Set<String> getLiteTopicSet() {
        return Collections.unmodifiableSet(liteTopicSet);
    }

    public long getVersion() {
        return version.get();
    }

    @Override
    public boolean isFifo() {
        // todo fifo 配置同步似乎有点问题，先这样写
        return true;
    }

    /**
     * 服务端处理完 client 发上去的 setting 之后，会写回同步
     * ClientActivity#processAndWriteClientSettings
     */
    //    @Override
    //    public void sync(apache.rocketmq.v2.Settings settings) {
    //        final apache.rocketmq.v2.Settings.PubSubCase pubSubCase = settings.getPubSubCase();
    //        if (!apache.rocketmq.v2.Settings.PubSubCase.INTEREST.equals(pubSubCase)) {
    //            log.error("[Bug] Issued settings not match with the client type, clientId={}, pubSubCase={}, "
    //                + "clientType={}", clientId, pubSubCase, clientType);
    //            return;
    //        }
    //        if (settings.hasBackoffPolicy()) {
    //            final RetryPolicy backoffPolicy = settings.getBackoffPolicy();
    //            switch (backoffPolicy.getStrategyCase()) {
    //                case EXPONENTIAL_BACKOFF:
    //                    retryPolicy = ExponentialBackoffRetryPolicy.fromProtobuf(backoffPolicy);
    //                    break;
    //                case CUSTOMIZED_BACKOFF:
    //                    retryPolicy = CustomizedBackoffRetryPolicy.fromProtobuf(backoffPolicy);
    //                    break;
    //                default:
    //                    throw new IllegalArgumentException("Unrecognized backoff policy strategy.");
    //            }
    //        }
    //    }
    @ExcludeFromJacocoGeneratedReport
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
            .add("clientId", clientId)
            .add("clientType", clientType)
            .add("accessPoint", accessPoint)
            .add("requestTimeout", requestTimeout)
            .add("bindTopic", bindTopic)
            .add("group", group)
            .add("interestSet", liteTopicSet)
            .toString();
    }
}
