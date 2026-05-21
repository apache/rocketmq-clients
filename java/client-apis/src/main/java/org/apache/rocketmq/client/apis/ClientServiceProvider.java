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
import org.apache.rocketmq.client.apis.consumer.LitePushConsumerBuilder;
import org.apache.rocketmq.client.apis.consumer.LiteSimpleConsumerBuilder;
import org.apache.rocketmq.client.apis.consumer.PushConsumerBuilder;
import org.apache.rocketmq.client.apis.consumer.SimpleConsumerBuilder;
import org.apache.rocketmq.client.apis.message.MessageBuilder;
import org.apache.rocketmq.client.apis.producer.ProducerBuilder;

/**
 * Service provider to seek client, which load client according to
 * <a href="https://en.wikipedia.org/wiki/Service_provider_interface">Java SPI mechanism</a>.
 */
public interface ClientServiceProvider {

    /**
     * To avoid potential concurrency issues, the {@link #loadService()} logic
     * has been changed to use lazy initialization with caching:
     * <p>
     * 1. Lazy loading + caching:
     * - On the first call, the implementation is loaded via {@link ServiceLoader}
     * and cached in {@link Holder#INSTANCE};
     * - Subsequent calls simply return the cached instance.
     * <p>
     * 2. If you need the old behavior (i.e., always load through ServiceLoader
     * each time), you can call {@link #doLoad()} directly:
     * - {@link #doLoad()} does not cache anything; it creates a new ServiceLoader
     * and loads an implementation on every call;
     * - You are responsible for handling any concurrency control when using
     * {@link #doLoad()} directly.
     */

    class Holder {
        static volatile ClientServiceProvider INSTANCE;

        private Holder() {
            // prevents instantiation
        }
    }

    static ClientServiceProvider loadService() {
        ClientServiceProvider inst = Holder.INSTANCE;
        if (inst != null) {
            return inst;
        }
        synchronized (ClientServiceProvider.class) {
            if (Holder.INSTANCE != null) {
                return Holder.INSTANCE;
            }
            Holder.INSTANCE = doLoad();
            return Holder.INSTANCE;
        }
    }

    static ClientServiceProvider doLoad() {
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
     * Get the lite push consumer builder by the current provider.
     *
     * @return the lite push consumer builder instance.
     */
    LitePushConsumerBuilder newLitePushConsumerBuilder();

    /**
     * Get the lite simple consumer builder by the current provider.
     *
     * @return the lite simple consumer builder instance.
     */
    LiteSimpleConsumerBuilder newLiteSimpleConsumerBuilder();

    /**
     * Get the simple consumer builder by the current provider.
     *
     * @return the simple consumer builder instance.
     */
    SimpleConsumerBuilder newSimpleConsumerBuilder();
}
