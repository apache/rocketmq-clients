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

package org.apache.rocketmq.client.impl.consumer;

import com.google.common.base.Optional;
import org.apache.rocketmq.client.consumer.MessageModel;
import org.apache.rocketmq.client.message.MessageQueue;

/**
 * Allow custom offset for consumption in {@link MessageModel#BROADCASTING}. Make it possible for consumer to decide
 * the initial offset to pull and consume.
 */
public interface OffsetStore {
    /**
     * Start the store, warm-up some resources.
     */
    void start();

    /**
     * Shutdown the store.
     */
    void shutdown();

    /**
     * Invoked while offset is updated.
     *
     * @param mq     offset owner.
     * @param offset next offset of {@link MessageQueue}
     */
    void updateOffset(MessageQueue mq, long offset);

    /**
     * Read offset from disk or other external storage.
     *
     * @param mq offset owner.
     * @return the next offset to pull and consume. or {@link Optional#absent()} if no offset exists
     */
    Optional<Long> readOffset(MessageQueue mq);
}
