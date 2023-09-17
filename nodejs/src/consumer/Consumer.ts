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

import { Message, Status } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  AckMessageRequest,
  ChangeInvisibleDurationRequest,
  ReceiveMessageRequest, ReceiveMessageResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { MessageView } from '../message';
import { MessageQueue } from '../route';
import { StatusChecker } from '../exception';
import { BaseClient, BaseClientOptions } from '../client';
import { createDuration, createResource } from '../util';
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
    const messages = messageList.map(message => new MessageView(message, mq, transportDeliveryTimestamp));
    return messages;
  }

  protected async ackMessage(messageView: MessageView) {
    const endpoints = messageView.endpoints;
    const request = new AckMessageRequest()
      .setGroup(createResource(this.consumerGroup))
      .setTopic(createResource(messageView.topic));
    request.addEntries()
      .setMessageId(messageView.messageId)
      .setReceiptHandle(messageView.receiptHandle);
    const res = await this.rpcClientManager.ackMessage(endpoints, request, this.requestTimeout);
    // FIXME: handle fail ack
    const response = res.toObject();
    StatusChecker.check(response.status);
    return response.entriesList;
  }

  protected async invisibleDuration(messageView: MessageView, invisibleDuration: number) {
    const request = new ChangeInvisibleDurationRequest()
      .setGroup(createResource(this.consumerGroup))
      .setTopic(createResource(messageView.topic))
      .setReceiptHandle(messageView.receiptHandle)
      .setInvisibleDuration(createDuration(invisibleDuration))
      .setMessageId(messageView.messageId);

    const res = await this.rpcClientManager.changeInvisibleDuration(messageView.endpoints, request, this.requestTimeout);
    const response = res.toObject();
    StatusChecker.check(response.status);
    return response.receiptHandle;
  }
}
