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

package org.apache.rocketmq.client.consumer;

import com.google.common.util.concurrent.ListenableFuture;
import java.util.List;
import org.apache.rocketmq.client.exception.ClientException;
import org.apache.rocketmq.client.impl.consumer.PullConsumerImpl;
import org.apache.rocketmq.client.message.MessageQueue;
import org.apache.rocketmq.client.remoting.CredentialsProvider;

public class DefaultMQPullConsumer {
    private final PullConsumerImpl impl;

    public DefaultMQPullConsumer(final String consumerGroup) {
        this.impl = new PullConsumerImpl(consumerGroup);
    }

    public void setConsumerGroup(String group) throws ClientException {
        this.impl.setGroup(group);
    }

    public String getConsumerGroup() {
        return this.impl.getGroup();
    }

    public void start() throws ClientException {
        this.impl.start();
    }

    public void shutdown() {
        this.impl.shutdown();
    }

    public void setNamesrvAddr(String namesrvAddr) throws ClientException {
        this.impl.setNamesrvAddr(namesrvAddr);
    }

    public void setMessageTracingEnabled(boolean tracingEnabled) {
        this.impl.setMessageTracingEnabled(tracingEnabled);
    }

    public ListenableFuture<List<MessageQueue>> queuesFor(String topic) {
        return this.impl.getQueuesFor(topic);
    }

    public ListenableFuture<Long> queryOffset(OffsetQuery offsetQuery) {
        return this.impl.queryOffset(offsetQuery);
    }

    public ListenableFuture<PullMessageResult> pull(PullMessageQuery pullMessageQuery) {
        return this.impl.pull(pullMessageQuery);
    }

    public void pull(PullMessageQuery pullMessageQuery, final PullCallback callback) {
        this.impl.pull(pullMessageQuery, callback);
    }

    public void setArn(String arn) {
        this.impl.setArn(arn);
    }

    public void setCredentialsProvider(CredentialsProvider provider) {
        this.impl.setCredentialsProvider(provider);
    }
}
