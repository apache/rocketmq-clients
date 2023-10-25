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

import assert from 'node:assert';
import { setTimeout } from 'node:timers/promises';
import {
  ClientType,
  MessageType,
  TransactionResolution,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  EndTransactionRequest,
  HeartbeatRequest,
  NotifyClientTerminationRequest,
  RecoverOrphanedTransactionCommand,
  SendMessageRequest,
} from '../../proto/apache/rocketmq/v2/service_pb';
import {
  Endpoints,
  MessageQueue,
  TopicRouteData,
} from '../route';
import {
  ExponentialBackoffRetryPolicy,
} from '../retry';
import { StatusChecker, TooManyRequestsException } from '../exception';
import { BaseClient, BaseClientOptions, Settings } from '../client';
import { PublishingMessage, MessageOptions, MessageView, Message } from '../message';
import { PublishingSettings } from './PublishingSettings';
import { TransactionChecker } from './TransactionChecker';
import { PublishingLoadBalancer } from './PublishingLoadBalancer';
import { SendReceipt } from './SendReceipt';
import { Transaction } from './Transaction';
import { createResource } from '../util';

export interface ProducerOptions extends BaseClientOptions {
  topic?: string | string[];
  maxAttempts?: number;
  checker?: TransactionChecker;
}

export class Producer extends BaseClient {
  #publishingSettings: PublishingSettings;
  #checker?: TransactionChecker;
  #publishingRouteDataCache = new Map<string, PublishingLoadBalancer>();

  constructor(options: ProducerOptions) {
    if (!options.topics && options.topic) {
      options.topics = Array.isArray(options.topic) ? options.topic : [ options.topic ];
    }
    super(options);
    // https://rocketmq.apache.org/docs/introduction/03limits/
    // Default max number of message sending retries is 3
    const retryPolicy = ExponentialBackoffRetryPolicy.immediatelyRetryPolicy(options.maxAttempts ?? 3);
    this.#publishingSettings = new PublishingSettings(this.clientId, this.endpoints, retryPolicy,
      this.requestTimeout, this.topics);
    this.#checker = options.checker;
  }

  get publishingSettings() {
    return this.#publishingSettings;
  }

  beginTransaction() {
    assert(this.#checker, 'Transaction checker should not be null');
    return new Transaction(this);
  }

  async endTransaction(endpoints: Endpoints, message: Message, messageId: string,
    transactionId: string, resolution: TransactionResolution) {
    const request = new EndTransactionRequest()
      .setMessageId(messageId)
      .setTransactionId(transactionId)
      .setTopic(createResource(message.topic))
      .setResolution(resolution);
    const response = await this.rpcClientManager.endTransaction(endpoints, request, this.requestTimeout);
    StatusChecker.check(response.getStatus()?.toObject());
  }

  async onRecoverOrphanedTransactionCommand(endpoints: Endpoints, command: RecoverOrphanedTransactionCommand) {
    const transactionId = command.getTransactionId();
    const messagePB = command.getMessage()!;
    const messageId = messagePB.getSystemProperties()!.getMessageId();
    if (!this.#checker) {
      this.logger.error('No transaction checker registered, ignore it, messageId=%s, transactionId=%s, endpoints=%s, clientId=%s',
        messageId, transactionId, endpoints, this.clientId);
      return;
    }
    let messageView: MessageView;
    try {
      messageView = new MessageView(messagePB);
    } catch (err) {
      this.logger.error('[Bug] Failed to decode message during orphaned transaction message recovery, messageId=%s, transactionId=%s, endpoints=%s, clientId=%s, error=%s',
        messageId, transactionId, endpoints, this.clientId, err);
      return;
    }

    try {
      const resolution = await this.#checker.check(messageView);
      if (resolution === null || resolution === TransactionResolution.TRANSACTION_RESOLUTION_UNSPECIFIED) {
        return;
      }
      await this.endTransaction(endpoints, messageView, messageId, transactionId, resolution);
      this.logger.info('Recover orphaned transaction message success, transactionId=%s, resolution=%s, messageId=%s, clientId=%s',
        transactionId, resolution, messageId, this.clientId);
    } catch (err) {
      this.logger.error('Exception raised while checking the transaction, messageId=%s, transactionId=%s, endpoints=%s, clientId=%s, error=%s',
        messageId, transactionId, endpoints, this.clientId, err);
      return;
    }
  }

  protected getSettings(): Settings {
    return this.#publishingSettings;
  }

  protected wrapHeartbeatRequest(): HeartbeatRequest {
    return new HeartbeatRequest()
      .setClientType(ClientType.PRODUCER);
  }

  protected wrapNotifyClientTerminationRequest(): NotifyClientTerminationRequest {
    return new NotifyClientTerminationRequest();
  }

  async send(message: MessageOptions, transaction?: Transaction) {
    if (!transaction) {
      const sendReceipts = await this.#send([ message ], false);
      return sendReceipts[0];
    }

    const publishingMessage = transaction.tryAddMessage(message);
    const sendReceipts = await this.#send([ message ], true);
    const sendReceipt = sendReceipts[0];
    transaction.tryAddReceipt(publishingMessage, sendReceipt);
    return sendReceipt;
  }

  async #send(messages: MessageOptions[], txEnabled: boolean) {
    const pubMessages: PublishingMessage[] = [];
    const topics = new Set<string>();
    for (const message of messages) {
      pubMessages.push(new PublishingMessage(message, this.#publishingSettings, txEnabled));
      topics.add(message.topic);
    }
    if (topics.size > 1) {
      throw new TypeError(`Messages to send have different topics=${JSON.stringify(topics)}`);
    }
    const topic = pubMessages[0].topic;
    const messageType = pubMessages[0].messageType;
    const messageGroup = pubMessages[0].messageGroup;
    const messageTypes = new Set(pubMessages.map(m => m.messageType));
    if (messageTypes.size > 1) {
      throw new TypeError(`Messages to send have different types=${JSON.stringify(messageTypes)}`);
    }

    // Message group must be same if message type is FIFO, or no need to proceed.
    if (messageType === MessageType.FIFO) {
      const messageGroups = new Set(pubMessages.map(m => m.messageGroup!));
      if (messageGroups.size > 1) {
        throw new TypeError(`FIFO messages to send have message groups, messageGroups=${JSON.stringify(messageGroups)}`);
      }
    }

    // Get publishing topic route.
    const loadBalancer = await this.#getPublishingLoadBalancer(topic);
    // Prepare the candidate message queue(s) for retry-sending in advance.
    const candidates = messageGroup ? [ loadBalancer.takeMessageQueueByMessageGroup(messageGroup) ] :
      this.#takeMessageQueues(loadBalancer);
    return await this.#send0(topic, messageType, candidates, pubMessages, 1);
  }

  #wrapSendMessageRequest(pubMessages: PublishingMessage[], mq: MessageQueue) {
    const request = new SendMessageRequest();
    for (const pubMessage of pubMessages) {
      request.addMessages(pubMessage.toProtobuf(mq));
    }
    return request;
  }

  /**
   * Isolate specified Endpoints
   */
  #isolate(endpoints: Endpoints) {
    this.isolated.set(endpoints.facade, endpoints);
  }

  async #send0(topic: string, messageType: MessageType, candidates: MessageQueue[],
    messages: PublishingMessage[], attempt: number): Promise<SendReceipt[]> {
    // Calculate the current message queue.
    const index = (attempt - 1) % candidates.length;
    const mq = candidates[index];
    const acceptMessageTypes = mq.acceptMessageTypesList;
    if (this.#publishingSettings.isValidateMessageType() && !acceptMessageTypes.includes(messageType)) {
      throw new TypeError('Current message type not match with ' +
        'topic accept message types, topic=' + topic + ', actualMessageType=' + messageType + ', ' +
        'acceptMessageTypes=' + JSON.stringify(acceptMessageTypes));
    }
    const endpoints = mq.broker.endpoints;
    const maxAttempts = this.#getRetryPolicy().getMaxAttempts();
    const request = this.#wrapSendMessageRequest(messages, mq);
    let sendReceipts: SendReceipt[] = [];
    try {
      const response = await this.rpcClientManager.sendMessage(endpoints, request, this.requestTimeout);
      sendReceipts = SendReceipt.processResponseInvocation(mq, response);
    } catch (err) {
      const messageIds = messages.map(m => m.messageId);
      // Isolate endpoints because of sending failure.
      this.#isolate(endpoints);
      if (attempt >= maxAttempts) {
        // No need more attempts.
        this.logger.error('Failed to send message(s) finally, run out of attempt times, maxAttempts=%s, attempt=%s, topic=%s, messageId(s)=%s, endpoints=%s, clientId=%s, error=%s',
          maxAttempts, attempt, topic, messageIds, endpoints, this.clientId, err);
        throw err;
      }
      // No need more attempts for transactional message.
      if (messageType === MessageType.TRANSACTION) {
        this.logger.error('Failed to send transactional message finally, maxAttempts=%s, attempt=%s, topic=%s, messageId(s)=%s, endpoints=%s, clientId=%s, error=%s',
          maxAttempts, attempt, topic, messageIds, endpoints, this.clientId, err);
        throw err;
      }
      // Try to do more attempts.
      const nextAttempt = 1 + attempt;
      // Retry immediately if the request is not throttled.
      if (!(err instanceof TooManyRequestsException)) {
        this.logger.warn('Failed to send message, would attempt to resend right now, maxAttempts=%s, attempt=%s, topic=%s, messageId(s)=%s, endpoints=%s, clientId=%s, error=%s',
          maxAttempts, attempt, topic, messageIds, endpoints, this.clientId, err);
        return this.#send0(topic, messageType, candidates, messages, nextAttempt);
      }
      const delay = this.#getRetryPolicy().getNextAttemptDelay(nextAttempt);
      this.logger.warn('Failed to send message due to too many requests, would attempt to resend after %sms, maxAttempts=%s, attempt=%s, topic=%s, messageId(s)=%s, endpoints=%s, clientId=%s, error=%s',
        delay, maxAttempts, attempt, topic, messageIds, endpoints, this.clientId, err);
      await setTimeout(delay);
      return this.#send0(topic, messageType, candidates, messages, nextAttempt);
    }

    // Resend message(s) successfully.
    if (attempt > 1) {
      const messageIds = sendReceipts.map(r => r.messageId);
      this.logger.info('Resend message successfully, topic=%s, messageId(s)=%j, maxAttempts=%s, attempt=%s, endpoints=%s, clientId=%s',
        topic, messageIds, maxAttempts, attempt, endpoints, this.clientId);
    }
    // Send message(s) successfully on first attempt, return directly.
    return sendReceipts;
  }

  async #getPublishingLoadBalancer(topic: string) {
    let loadBalancer = this.#publishingRouteDataCache.get(topic);
    if (!loadBalancer) {
      const topicRouteData = await this.getRouteData(topic);
      loadBalancer = this.#updatePublishingLoadBalancer(topic, topicRouteData);
    }
    return loadBalancer;
  }

  #updatePublishingLoadBalancer(topic: string, topicRouteData: TopicRouteData) {
    let loadBalancer = this.#publishingRouteDataCache.get(topic);
    if (loadBalancer) {
      loadBalancer = loadBalancer.update(topicRouteData);
    } else {
      loadBalancer = new PublishingLoadBalancer(topicRouteData);
    }
    this.#publishingRouteDataCache.set(topic, loadBalancer);
    return loadBalancer;
  }

  /**
   * Take message queue(s) from route for message publishing.
   */
  #takeMessageQueues(loadBalancer: PublishingLoadBalancer) {
    return loadBalancer.takeMessageQueues(this.isolated, this.#getRetryPolicy().getMaxAttempts());
  }

  #getRetryPolicy() {
    return this.#publishingSettings.getRetryPolicy()!;
  }
}
