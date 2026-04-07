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

import { MessageView } from '../message';
import { OffsetOption } from './OffsetOption';

/**
 * Similar to SimpleConsumer, but for lite topic.
 *
 * <p>LiteSimpleConsumer provides lightweight message consumption with reduced
 * metadata and storage overhead. It supports dynamic subscription management
 * for lite topics.</p>
 */
export interface LiteSimpleConsumer {
  /**
   * Get the load balancing group for the lite simple consumer.
   *
   * @return consumer load balancing group.
   */
  getConsumerGroup(): string;

  /**
   * Subscribe to a lite topic.
   *
   * <p>The subscribeLite() method initiates network requests and performs quota verification,
   * so it may fail. It's important to check the result of this call to ensure that the
   * subscription was successfully added. Possible failure scenarios include:</p>
   * <ol>
   *   <li>Network request errors, which can be retried.</li>
   *   <li>Quota verification failures, indicated by LiteSubscriptionQuotaExceededException.
   *       In this case, evaluate whether the quota is insufficient and promptly unsubscribe
   *       from unused subscriptions using unsubscribeLite() to free up resources.</li>
   * </ol>
   *
   * @param liteTopic the name of the lite topic to subscribe
   * @throws ClientException if an error occurs during subscription
   */
  subscribeLite(liteTopic: string): Promise<void>;

  /**
   * Subscribe to a lite topic with offsetOption to specify the consume from offset.
   *
   * @param liteTopic the name of the lite topic to subscribe
   * @param offsetOption the consume from offset
   * @throws ClientException if an error occurs during subscription
   */
  subscribeLite(liteTopic: string, offsetOption: OffsetOption): Promise<void>;

  /**
   * Unsubscribe from a lite topic.
   *
   * @param liteTopic the name of the lite topic to unsubscribe from
   * @throws ClientException if an error occurs during unsubscription
   */
  unsubscribeLite(liteTopic: string): Promise<void>;

  /**
   * Get the lite topic immutable set.
   *
   * @return lite topic immutable set.
   */
  getLiteTopicSet(): Set<string>;

  /**
   * Receive messages synchronously.
   *
   * <p>This method returns immediately if there are messages available.
   * Otherwise, it will await the passed timeout. If the timeout expires, an empty list will be returned.</p>
   *
   * @param maxMessageNum max message num of server returned
   * @param invisibleDuration set the invisible duration of messages to return from the server
   * @return list of message view
   */
  receive(maxMessageNum: number, invisibleDuration: number): Promise<MessageView[]>;

  /**
   * Acknowledge a message synchronously.
   *
   * <p>Duplicate ack request does not take effect and throw an exception.</p>
   *
   * @param messageView special message view with handle want to ack
   */
  ack(messageView: MessageView): Promise<void>;

  /**
   * Changes the invisible duration of a specified message synchronously.
   *
   * <p>The origin invisible duration for a message decide by ack request.</p>
   * <p>Duplicate change requests will refresh the next visible time of this message to consumers.</p>
   *
   * @param messageView the message view to change invisible time
   * @param invisibleDuration new timestamp the message could be visible and re-consume which start from current time
   */
  changeInvisibleDuration(messageView: MessageView, invisibleDuration: number): Promise<void>;

  /**
   * Close the consumer and release all related resources.
   *
   * <p>Once consumer is closed, <strong>it could not be started once again.</strong></p>
   */
  close(): Promise<void>;
}
