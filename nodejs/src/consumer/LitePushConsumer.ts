/**
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

import { OffsetOption } from './OffsetOption';

/**
 * LitePushConsumer interface for consuming messages from lite topics.
 *
 * <p>LitePushConsumer is a specialized consumer designed for lightweight scenarios
 * with reduced metadata and storage overhead. It supports dynamic subscription
 * management for lite topics.</p>
 */
export interface LitePushConsumer {
  /**
   * Subscribe to a lite topic.
   *
   * <p>The subscribeLite() method initiates network requests and performs quota verification,
   * so it may fail. It's important to check the result of this call to ensure that the
   * subscription was successfully added. Possible failure scenarios include:</p>
   * <ul>
   *   <li>Network request errors, which can be retried.</li>
   *   <li>Quota verification failures, indicated by LiteSubscriptionQuotaExceededException.
   *       In this case, evaluate whether the quota is insufficient and promptly unsubscribe
   *       from unused subscriptions using unsubscribeLite() to free up resources.</li>
   * </ul>
   *
   * @param liteTopic - The name of the lite topic to subscribe
   * @throws ClientException if an error occurs during subscription
   */
  subscribeLite(liteTopic: string): Promise<void>;

  /**
   * Subscribe to a lite topic with consumeFromOption to specify the consume from offset.
   *
   * @param liteTopic - The name of the lite topic to subscribe
   * @param offsetOption - The consume from offset option
   * @throws ClientException if an error occurs during subscription
   */
  subscribeLite(liteTopic: string, offsetOption: OffsetOption): Promise<void>;

  /**
   * Unsubscribe from a lite topic.
   *
   * @param liteTopic - The name of the lite topic to unsubscribe from
   * @throws ClientException if an error occurs during unsubscription
   */
  unsubscribeLite(liteTopic: string): Promise<void>;

  /**
   * Get the lite topic immutable set.
   *
   * @return Lite topic immutable set
   */
  getLiteTopicSet(): Set<string>;

  /**
   * Get the load balancing group for the consumer.
   *
   * @return Consumer load balancing group
   */
  getConsumerGroup(): string;

  /**
   * Close the consumer and release all related resources.
   *
   * <p>Once the consumer is closed, <strong>it could not be started once again.</strong>
   * We maintain an FSM (finite-state machine) to record the different states for each
   * push consumer.</p>
   */
  close(): Promise<void>;
}
