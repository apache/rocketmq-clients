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
import { LiteSimpleConsumerImpl } from './LiteSimpleConsumerImpl';

/**
 * LiteSimpleConsumer interface for consuming messages from lite topics with
 * explicit receive/ack control.
 *
 * <p>Similar to SimpleConsumer, but bound to a single parent topic and able to
 * (un)subscribe lite topics dynamically through the lite subscription sync
 * protocol.</p>
 */
export interface LiteSimpleConsumer {
  /**
   * Get the load balancing group for the lite simple consumer.
   *
   * @return Consumer load balancing group
   */
  getConsumerGroup(): string;

  /**
   * Subscribe to a lite topic.
   *
   * <p>The subscribeLite() method initiates network requests and performs quota
   * verification, so it may fail. It's important to check the result of this
   * call to ensure that the subscription was successfully added. Possible
   * failure scenarios include:</p>
   * <ul>
   *   <li>Network request errors, which can be retried.</li>
   *   <li>Quota verification failures, indicated by LiteSubscriptionQuotaExceededException.
   *       In this case, evaluate whether the quota is insufficient and promptly
   *       unsubscribe from unused subscriptions using unsubscribeLite() to free
   *       up resources.</li>
   * </ul>
   *
   * @param liteTopic - The name of the lite topic to subscribe
   * @throws ClientException if an error occurs during subscription
   */
  subscribeLite(liteTopic: string): Promise<void>;

  /**
   * Subscribe to a lite topic with an offset option to specify the consume from
   * offset.
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
   * Fetch messages from the server synchronously.
   *
   * <p>This method returns immediately if there are messages available.
   * Otherwise, it will await the passed timeout. If the timeout expires, an
   * empty list will be returned.</p>
   *
   * @param maxMessageNum - Max message num of server returned
   * @param invisibleDuration - Set the invisibleDuration of messages to return
   * @return List of message views
   */
  receive(maxMessageNum?: number, invisibleDuration?: number): Promise<MessageView[]>;

  /**
   * Ack the consumption of the message which is returned by receive().
   *
   * @param messageView - Message view with the receipt handle to ack
   * @throws ClientException if an error occurs during ack
   */
  ack(messageView: MessageView): Promise<void>;

  /**
   * Change the invisible duration of a received message.
   *
   * @param messageView - Message view to change invisible duration
   * @param invisibleDuration - New invisible duration
   * @throws ClientException if an error occurs
   */
  changeInvisibleDuration(messageView: MessageView, invisibleDuration: number): Promise<void>;

  /**
   * Close the consumer and release all related resources.
   *
   * <p>Once the consumer is closed, <strong>it could not be started once
   * again.</strong></p>
   */
  close(): Promise<void>;
}

export interface LiteSimpleConsumerOptions extends BaseClientOptions {
  consumerGroup: string;
  /**
   * The parent topic the lite consumer binds to. All lite topics subscribed by
   * this consumer must belong to this parent topic.
   */
  bindTopic: string;
  /**
   * set await duration for long-polling, default is 30000ms
   */
  awaitDuration?: number;
  /**
   * max retry attempts for temporary errors (e.g., internal server error), default is 3
   */
  maxRetryAttempts?: number;
}

const CONSUMER_GROUP_PATTERN = /^[a-zA-Z0-9_-]+$/;

/**
 * LiteSimpleConsumer builder class.
 *
 * <p>This class provides a fluent API for configuring and creating lite simple
 * consumers with explicit receive/ack control over lite topics.</p>
 */
export class LiteSimpleConsumerBuilder {
  private options: Partial<LiteSimpleConsumerOptions> = {};

  /**
   * Set the bind topic for the lite simple consumer.
   *
   * @param bindTopic - The parent topic to bind
   * @return This builder instance
   * @throws Error if bindTopic is blank
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
   * @param options - Client configuration options
   * @return This builder instance
   * @throws Error if options is null/undefined
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
   * @param consumerGroup - Consumer group name
   * @return This builder instance
   * @throws Error if consumerGroup is null or doesn't match the pattern
   */
  setConsumerGroup(consumerGroup: string): LiteSimpleConsumerBuilder {
    if (!consumerGroup) {
      throw new Error('consumerGroup should not be null');
    }
    if (!CONSUMER_GROUP_PATTERN.test(consumerGroup)) {
      throw new Error(`consumerGroup does not match the pattern ${CONSUMER_GROUP_PATTERN.source}`);
    }
    this.options.consumerGroup = consumerGroup;
    return this;
  }

  /**
   * Set the await duration for long-polling receive requests.
   *
   * @param awaitDuration - Maximum time to block when no message is available
   * @return This builder instance
   * @throws Error if awaitDuration is not positive
   */
  setAwaitDuration(awaitDuration: number): LiteSimpleConsumerBuilder {
    if (awaitDuration <= 0) {
      throw new Error('awaitDuration should be positive');
    }
    this.options.awaitDuration = awaitDuration;
    return this;
  }

  /**
   * Finalize the build of LiteSimpleConsumer and start.
   *
   * <p>This method will block until the lite simple consumer starts
   * successfully.</p>
   *
   * @return Promise resolving to started LiteSimpleConsumer instance
   * @throws Error if required parameters are not set
   */
  async build(): Promise<LiteSimpleConsumer> {
    if (!this.options.endpoints) {
      throw new Error('clientConfiguration has not been set yet');
    }
    if (!this.options.consumerGroup) {
      throw new Error('consumerGroup has not been set yet');
    }
    if (!this.options.bindTopic) {
      throw new Error('bindTopic has not been set yet');
    }

    const options: LiteSimpleConsumerOptions = {
      endpoints: this.options.endpoints,
      namespace: this.options.namespace ?? '',
      consumerGroup: this.options.consumerGroup!,
      bindTopic: this.options.bindTopic,
      awaitDuration: this.options.awaitDuration,
      maxRetryAttempts: this.options.maxRetryAttempts,
      sslEnabled: this.options.sslEnabled,
      sessionCredentials: this.options.sessionCredentials,
      requestTimeout: this.options.requestTimeout,
      logger: this.options.logger,
    };

    const liteSimpleConsumer = new LiteSimpleConsumerImpl(options);
    await liteSimpleConsumer.startup();
    return liteSimpleConsumer;
  }
}
