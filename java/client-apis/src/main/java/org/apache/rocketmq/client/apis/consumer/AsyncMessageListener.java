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

import java.util.concurrent.CompletionStage;
import org.apache.rocketmq.client.apis.message.MessageView;

/**
 * Processes a push-consumer message whose result is available asynchronously.
 *
 * <p>The consumer acknowledges a message only after the returned stage completes with
 * {@link ConsumeResult#SUCCESS}. An exception, cancellation, null stage or null result is treated as
 * {@link ConsumeResult#FAILURE}. Completing the stage reports the processing result, not completion of the
 * acknowledgement RPC. Applications must remain idempotent because messages may be delivered more than once.
 *
 * <p>Return promptly after arranging asynchronous work. Outstanding stages count towards the configured consumption
 * concurrency. The consumer retains the message in its cache and uses the existing push-consumer server-side
 * auto-renew protocol while waiting; this does not extend the server's maximum processing duration.
 *
 * <p>Closing the consumer waits for outstanding processing and acknowledgement operations. Every returned stage must
 * eventually complete, and application executors must remain available until the consumer has closed.
 */
@FunctionalInterface
public interface AsyncMessageListener {
    /**
     * Start processing a message and return its eventual result.
     *
     * @param messageView message to process.
     * @return a non-null stage completed after message processing finishes.
     */
    CompletionStage<ConsumeResult> consumeAsync(MessageView messageView);
}
