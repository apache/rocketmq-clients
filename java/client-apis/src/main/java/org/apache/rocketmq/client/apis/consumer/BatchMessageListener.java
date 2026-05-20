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

package org.apache.rocketmq.client.apis.consumer;

import java.util.List;
import org.apache.rocketmq.client.apis.message.MessageView;

/**
 * BatchMessageListener is used for the push consumer to process message consumption in batches.
 *
 * <p>Refer to {@link PushConsumer}, push consumer will get messages from the server, buffer them
 * locally according to the {@link BatchPolicy}, and dispatch the accumulated batch to the backend
 * thread pool for consumption.
 *
 * <p>Messages are accumulated until any of the following batch conditions is met:
 * <ol>
 *   <li>The number of buffered messages reaches {@link BatchPolicy#getMaxBatchCount()}.</li>
 *   <li>The total body size (in bytes) of buffered messages reaches {@link BatchPolicy#getMaxBatchBytes()}.</li>
 *   <li>The time elapsed since the first buffered message reaches {@link BatchPolicy#getMaxWaitTime()}.</li>
 * </ol>
 */
public interface BatchMessageListener {
    /**
     * The callback interface to consume a batch of messages.
     *
     * <p>You should process the list of {@link MessageView} and return the corresponding
     * {@link ConsumeResult}. The consumption is successful only when {@link ConsumeResult#SUCCESS}
     * is returned; returning {@code null} or throwing an exception causes the entire batch to be
     * marked as consumption failure.
     *
     * <p><strong>Note:</strong> The returned {@link ConsumeResult} applies to <em>all</em> messages
     * in the batch. If the batch fails, all messages in the batch will be retried.
     *
     * @param messageViews the batch of messages to consume; never {@code null} or empty.
     * @return the consume result for the entire batch.
     */
    ConsumeResult consume(List<MessageView> messageViews);
}
