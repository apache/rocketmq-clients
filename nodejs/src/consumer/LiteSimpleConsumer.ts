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

import { BaseClientOptions } from '../client';
import { MessageView } from '../message';
import { OffsetOption } from './OffsetOption';
import { LiteSimpleConsumerImpl, LiteSimpleConsumerOptions } from './LiteSimpleConsumerImpl';

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

/**
 * LiteSimpleConsumer builder class.
 *
 * <p>This class provides a fluent API for configuring and creating
 * lite simple consumers with reduced overhead for lightweight scenarios.</p>
 */
export class LiteSimpleConsumerBuilder {
  private options: Partial<LiteSimpleConsumerOptions> = {};

  /**
   * Set the bind topic for the lite simple consumer.
   *
   * @param bindTopic the parent topic that lite topics belong to
   * @return this builder
   */
  bindTopic(bindTopic: string): LiteSimpleConsumerBuilder {
    if (!bindTopic || bindTopic.trim().length === 0) {
      throw new Error('bindTopic should not be blank');
    }
    this.options.bindTopic = bindTopic;
    return this;
  }

  /**
   * Set the client configuration.
   *
   * @param options the client configuration options
   * @return this builder
   */
  setClientConfiguration(options: BaseClientOptions): LiteSimpleConsumerBuilder {
    if (!options) {
      throw new Error('clientConfiguration should not be null');
    }
    Object.assign(this.options, options);
    return this;
  }

  /**
   * Set the consumer group.
   *
   * <p>Note: For lite consumers, the consumer group should NOT start with 'GID-' prefix.</p>
   *
   * @param consumerGroup the consumer group name
   * @return this builder
   */
  setConsumerGroup(consumerGroup: string): LiteSimpleConsumerBuilder {
    if (!consumerGroup) {
      throw new Error('consumerGroup should not be null');
    }
    // Validate consumer group format (should not start with GID-)
    if (consumerGroup.startsWith('GID-')) {
      throw new Error(
        'Lite consumer group should not start with "GID-" prefix. ' +
        'Please use a different group name without the prefix.');
    }
    this.options.consumerGroup = consumerGroup;
    return this;
  }

  /**
   * Set the await duration for long-polling.
   *
   * @param awaitDuration the await duration in milliseconds
   * @return this builder
   */
  setAwaitDuration(awaitDuration: number): LiteSimpleConsumerBuilder {
    if (awaitDuration <= 0) {
      throw new Error('awaitDuration should be greater than 0');
    }
    this.options.awaitDuration = awaitDuration;
    return this;
  }

  /**
   * Start up the LiteSimpleConsumer.
   *
   * @return the started LiteSimpleConsumer instance
   * @throws Error if required parameters are not set
   */
  async startup(): Promise<LiteSimpleConsumer> {
    // Validate required parameters
    if (!this.options.endpoints) {
      throw new Error('clientConfiguration has not been set yet');
    }
    if (!this.options.consumerGroup) {
      throw new Error('consumerGroup has not been set yet');
    }
    if (!this.options.awaitDuration) {
      throw new Error('awaitDuration has not been set yet');
    }
    if (!this.options.bindTopic) {
      throw new Error('bindTopic has not been set yet');
    }

    // Create options
    const options: any = {
      endpoints: this.options.endpoints,
      namespace: this.options.namespace ?? '',
      consumerGroup: this.options.consumerGroup,
      bindTopic: this.options.bindTopic,
      awaitDuration: this.options.awaitDuration,
      subscriptions: new Map(), // Will be set in constructor
      sslEnabled: this.options.sslEnabled,
      sessionCredentials: this.options.sessionCredentials,
      requestTimeout: this.options.requestTimeout,
      logger: this.options.logger,
    };

    // Create and start the consumer
    const consumer = new LiteSimpleConsumerImpl(options);
    await consumer.startup();

    return consumer;
  }
}
