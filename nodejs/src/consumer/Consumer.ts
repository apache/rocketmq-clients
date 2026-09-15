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

import { Code, Message, Status } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  AckMessageRequest,
  ChangeInvisibleDurationRequest,
  ReceiveMessageRequest, ReceiveMessageResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { ClientType } from '../../proto/apache/rocketmq/v2/definition_pb';
import { MessageView } from '../message';
import { MessageQueue } from '../route';
import { StatusChecker } from '../exception';
import { BaseClient, BaseClientOptions } from '../client';
import { createDuration, createResource } from '../util';
import {
  GeneralMessage,
  MessageHookPoints,
  MessageHookPointsStatus,
  MessageInterceptorContextImpl,
} from '../hook';
import { FilterExpression } from './FilterExpression';

export interface ConsumerOptions extends BaseClientOptions {
  consumerGroup: string;
}

export abstract class Consumer extends BaseClient {
  protected readonly consumerGroup: string;

  constructor(options: ConsumerOptions) {
    super(options);
    this.consumerGroup = options.consumerGroup;
  }

  protected wrapReceiveMessageRequest(batchSize: number, mq: MessageQueue,
    filterExpression: FilterExpression, invisibleDuration: number, longPollingTimeout: number) {
    return new ReceiveMessageRequest()
      .setGroup(createResource(this.consumerGroup))
      .setMessageQueue(mq.toProtobuf())
      .setFilterExpression(filterExpression.toProtobuf())
      .setLongPollingTimeout(createDuration(longPollingTimeout))
      .setBatchSize(batchSize)
      .setAutoRenew(false)
      .setInvisibleDuration(createDuration(invisibleDuration));
  }

  protected async receiveMessage(request: ReceiveMessageRequest, mq: MessageQueue, awaitDuration: number) {
    const endpoints = mq.broker.endpoints;
    const timeout = this.requestTimeout + awaitDuration;
    let status: Status.AsObject | undefined;

    try {
      this.logger.debug?.('Receiving messages from broker, topic=%s, endpoints=%s, batchSize=%d, clientId=%s',
        request.getMessageQueue()?.getTopic()?.getName(), endpoints, request.getBatchSize(), (this as any).clientId);

      const responses = await this.rpcClientManager.receiveMessage(endpoints, request, timeout);
      const messageList: Message[] = [];
      let transportDeliveryTimestamp: Date | undefined;

      for (const response of responses) {
        switch (response.getContentCase()) {
          case ReceiveMessageResponse.ContentCase.STATUS:
            status = response.getStatus()?.toObject();
            break;
          case ReceiveMessageResponse.ContentCase.MESSAGE:
            messageList.push(response.getMessage()!);
            break;
          case ReceiveMessageResponse.ContentCase.DELIVERY_TIMESTAMP:
            transportDeliveryTimestamp = response.getDeliveryTimestamp()?.toDate();
            break;
          default:
            // this.logger.warn("[Bug] Not recognized content for receive message response, mq={}, " +
            //                 "clientId={}, response={}", mq, clientId, response);
        }
      }

      StatusChecker.check(status);

      if (messageList.length > 0) {
        this.logger.debug?.('Received %d messages successfully, topic=%s, endpoints=%s, clientId=%s',
          messageList.length, request.getMessageQueue()?.getTopic()?.getName(), endpoints, (this as any).clientId);
      }

      const messages = messageList.map(message => new MessageView(message, mq, transportDeliveryTimestamp));
      // RECEIVE hook point, mirroring Java ProcessQueueImpl.
      const context = new MessageInterceptorContextImpl(MessageHookPoints.RECEIVE, MessageHookPointsStatus.OK);
      this.doBefore(context, messages);
      this.doAfter(MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), messages);
      return messages;
    } catch (err) {
      this.logger.error('Failed to receive messages, topic=%s, endpoints=%s, clientId=%s, error=%s',
        request.getMessageQueue()?.getTopic()?.getName(), endpoints, (this as any).clientId, err);
      // RECEIVE hook point with error status.
      const context = new MessageInterceptorContextImpl(MessageHookPoints.RECEIVE);
      this.doBefore(context, []);
      this.doAfter(MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.ERROR), []);
      throw err;
    }
  }

  protected async ackMessage(messageView: MessageView) {
    const endpoints = messageView.endpoints;
    const request = new AckMessageRequest()
      .setGroup(createResource(this.consumerGroup))
      .setTopic(createResource(messageView.topic));
    request.addEntries()
      .setMessageId(messageView.messageId)
      .setReceiptHandle(messageView.receiptHandle);
    // ACK hook point, mirroring Java ConsumerImpl#ackMessage.
    const context = new MessageInterceptorContextImpl(MessageHookPoints.ACK);
    const generalMessages = [ messageView ];
    this.doBefore(context, generalMessages);
    let hookStatus = MessageHookPointsStatus.OK;
    try {
      const res = await this.rpcClientManager.ackMessage(endpoints, request, this.requestTimeout);
      // FIXME: handle fail ack
      const response = res.toObject();
      hookStatus = response.status?.code === Code.OK ? MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
      StatusChecker.check(response.status);
      this.doAfter(MessageInterceptorContextImpl.withStatus(context, hookStatus), generalMessages);
      return response.entriesList;
    } catch (err) {
      this.doAfter(MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.ERROR), generalMessages);
      throw err;
    }
  }

  protected async invisibleDuration(messageView: MessageView, invisibleDuration: number) {
    const request = new ChangeInvisibleDurationRequest()
      .setGroup(createResource(this.consumerGroup))
      .setTopic(createResource(messageView.topic))
      .setReceiptHandle(messageView.receiptHandle)
      .setInvisibleDuration(createDuration(invisibleDuration))
      .setMessageId(messageView.messageId);

    // CHANGE_INVISIBLE_DURATION hook point, mirroring Java ConsumerImpl#changeInvisibleDuration.
    const context = new MessageInterceptorContextImpl(MessageHookPoints.CHANGE_INVISIBLE_DURATION);
    const generalMessages = [ messageView ];
    this.doBefore(context, generalMessages);
    let hookStatus = MessageHookPointsStatus.OK;
    try {
      const res = await this.rpcClientManager.changeInvisibleDuration(messageView.endpoints, request, this.requestTimeout);
      const response = res.toObject();
      hookStatus = response.status?.code === Code.OK ? MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
      StatusChecker.check(response.status);
      this.doAfter(MessageInterceptorContextImpl.withStatus(context, hookStatus), generalMessages);
      return response.receiptHandle;
    } catch (err) {
      this.doAfter(MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.ERROR), generalMessages);
      throw err;
    }
  }

  /**
   * Expose public methods for ProcessQueue to access RPC operations
   */
  async ackMessageViaRpc(endpoints: any, request: AckMessageRequest, timeout: number) {
    const res = await this.rpcClientManager.ackMessage(endpoints, request, timeout);
    return res;
  }

  async changeInvisibleDurationViaRpc(endpoints: any, request: ChangeInvisibleDurationRequest, timeout: number) {
    const res = await this.rpcClientManager.changeInvisibleDuration(endpoints, request, timeout);
    return res;
  }

  /**
   * Forward the message to the dead letter queue, mirroring Java
   * PushConsumerImpl#forwardMessageToDeadLetterQueue. The FORWARD_TO_DLQ hook
   * point is triggered around every RPC attempt.
   */
  async forwardMessageToDeadLetterQueueViaRpc(endpoints: any, request: any, timeout: number,
    messageView: MessageView) {
    // FORWARD_TO_DLQ hook point, mirroring Java PushConsumerImpl#forwardMessageToDeadLetterQueue.
    const context = new MessageInterceptorContextImpl(MessageHookPoints.FORWARD_TO_DLQ);
    const generalMessages: GeneralMessage[] = [ messageView ];
    this.doBefore(context, generalMessages);
    try {
      const response = await this.rpcClientManager.forwardMessageToDeadLetterQueue(endpoints, request, timeout);
      const hookStatus = response.getStatus()?.getCode() === Code.OK ?
        MessageHookPointsStatus.OK : MessageHookPointsStatus.ERROR;
      this.doAfter(MessageInterceptorContextImpl.withStatus(context, hookStatus), generalMessages);
      return response;
    } catch (err) {
      this.doAfter(MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.ERROR), generalMessages);
      throw err;
    }
  }

  /**
   * Get the consumer group name.
   *
   * @return Consumer group name
   */
  getConsumerGroup(): string {
    return this.consumerGroup;
  }

  /**
   * Check if this is a lite consumer.
   *
   * @return true if this is a LITE_PUSH_CONSUMER
   */
  protected isLiteConsumer(): boolean {
    const clientType = (this as any).getClientType?.();
    return clientType === ClientType.LITE_PUSH_CONSUMER;
  }
}
