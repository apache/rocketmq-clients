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
 * <p>Unlike {@link MessageListener} which processes messages one by one, this listener receives a batch of messages
 * and returns a single {@link ConsumeResult} that applies to the entire batch. If the result is
 * {@link ConsumeResult#SUCCESS}, all messages in the batch will be acknowledged. If the result is
 * {@link ConsumeResult#FAILURE}, all messages will go through the retry/dead-letter-queue logic.
 *
 * <p>The batch is formed based on the {@link BatchPolicy} configuration: messages are accumulated until any of the
 * following conditions is met:
 * <ul>
 *     <li>The number of buffered messages reaches {@link BatchPolicy#getMaxBatchSize()}</li>
 *     <li>The total bytes of buffered messages reaches {@link BatchPolicy#getMaxBatchBytes()}</li>
 *     <li>The time since the first message entered the buffer reaches {@link BatchPolicy#getMaxWaitTime()}</li>
 * </ul>
 *
 * <p><strong>Warning:</strong> The batch buffer is global across all message groups. In FIFO mode, a single batch
 * may contain messages from different message groups. If the batch fails and exhausts retries, all messages in the
 * batch (including messages from multiple message groups) will be forwarded to DLQ together, which may block
 * consumption progress for other message groups. This design prioritizes throughput over strict isolation.
 *
 * @see PushConsumer
 * @see BatchPolicy
 */
public interface BatchMessageListener {
    /**
     * The callback interface to consume a batch of messages.
     *
     * <p>You should process the list of {@link MessageView} and return the corresponding {@link ConsumeResult}.
     * The consumption is successful only when {@link ConsumeResult#SUCCESS} is returned. A null pointer return
     * or an exception thrown will be treated as {@link ConsumeResult#FAILURE}.
     *
     * @param messages the batch of messages to consume, guaranteed to be non-empty.
     * @return the consume result that applies to all messages in the batch.
     */
    ConsumeResult consume(List<MessageView> messages);
}
