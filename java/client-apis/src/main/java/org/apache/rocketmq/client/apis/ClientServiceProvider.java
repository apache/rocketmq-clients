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

import java.util.Iterator;
import java.util.ServiceLoader;
import org.apache.rocketmq.client.apis.consumer.PushConsumerBuilder;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumerBuilder;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;

/**
 * Service provider to seek client, which load client according to
 * <a href="https://en.wikipedia.org/wiki/Service_provider_interface">Java SPI mechanism</a>.
 */
public interface ClientServiceProvider {
    static ClientServiceProvider loadService() {
        final ServiceLoader<ClientServiceProvider> loaders = ServiceLoader.load(ClientServiceProvider.class);
        final Iterator<ClientServiceProvider> iterators = loaders.iterator();
        if (iterators.hasNext()) {
            return iterators.next();
        }
        throw new UnsupportedOperationException("Client service provider not found");
    }

    /**
     * Get the producer builder by the current provider.
     *
     * @return the producer builder instance.
     */
    ProducerBuilder newProducerBuilder();

    /**
     * Get the message builder by the current provider.
     *
     * @return the message builder instance.
     */
    MessageBuilder newMessageBuilder();

    /**
     * Get the push consumer builder by the current provider.
     *
     * @return the push consumer builder instance.
     */
    PushConsumerBuilder newPushConsumerBuilder();

    /**
     * Get the simple consumer builder by the current provider.
     *
     * @return the simple consumer builder instance.
     */
    SimpleConsumerBuilder newSimpleConsumerBuilder();
}
