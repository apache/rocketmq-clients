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

import { Endpoints, MessageQueue } from '../route';
import { Consumer, ConsumerOptions, ConsumeResult } from './Consumer';
import { FilterExpression } from './FilterExpression';
import { ProcessQueue } from './ProcessQueue';
import { ConsumeService, StandardConsumeService } from './ConsumeService';
import { MessageView } from '../message';
import { HeartbeatRequest, NotifyClientTerminationRequest, ReceiveMessageRequest } from '../../proto/apache/rocketmq/v2/service_pb';
import { ClientType } from '../../proto/apache/rocketmq/v2/definition_pb';
import { PushSubscriptionSettings } from './PushSubscriptionSettings';
import { createResource } from '../util';

export type MessageListener = (message: MessageView) => Promise<ConsumeResult>;

export class PushConsumer extends Consumer {
  private readonly subscriptionExpressions: Map<string, FilterExpression>;
  private readonly messageListener: MessageListener;
  private readonly processQueueTable: Map<string, ProcessQueue>;
  private readonly consumeService: ConsumeService;
  private scanAssignmentsTimer: NodeJS.Timeout | null = null;
  private readonly settings: PushSubscriptionSettings;

  constructor(options: ConsumerOptions, messageListener: MessageListener) {
    super(options);
    this.messageListener = messageListener;
    this.subscriptionExpressions = new Map();
    this.processQueueTable = new Map();
    this.consumeService = new StandardConsumeService(this.messageListener);
    this.settings = new PushSubscriptionSettings(this.namespace, this.clientId, new Endpoints(options.endpoints), this.consumerGroup, options.requestTimeout || 3000, this.subscriptionExpressions, 1024, 1024 * 1024);
  }

  subscribe(topic: string, filterExpression: FilterExpression) {
    this.subscriptionExpressions.set(topic, filterExpression);
    this.topics.add(topic);
  }

  async start() {
    await this.startup();
    this.scanAssignmentsTimer = setInterval(this.scanAssignments.bind(this), 5000);
  }

  async stop() {
    if (this.scanAssignmentsTimer) {
      clearInterval(this.scanAssignmentsTimer);
    }
    this.consumeService.shutdown();
    await this.shutdown();
  }

  private async scanAssignments() {
    for (const [topic, filterExpression] of this.subscriptionExpressions.entries()) {
      try {
        const topicRouteData = await this.getRouteData(topic);
        const assignments = topicRouteData.messageQueues;
        // replace with origin topic
          for (const assignment of assignments) {
              assignment.topic.name = topic;
          }
        this.syncProcessQueue(topic, assignments, filterExpression);
      } catch (error) {
        this.logger.error(`Failed to scan assignments for topic ${topic}`, error);
      }
    }
  }

  private syncProcessQueue(topic: string, assignments: any[], filterExpression: FilterExpression) {
    const latest = new Set<string>();
    for (const assignment of assignments) {
     /*  if (assignment.topic.name !== topic) {
        continue;
      } */
      const queueId = `${assignment.topic.name}-${assignment.queueId}`;
      latest.add(queueId);
      if (!this.processQueueTable.has(queueId)) {
        const processQueue = new ProcessQueue(this, assignment, filterExpression, this.consumeService);
        this.processQueueTable.set(queueId, processQueue);
        processQueue.fetchMessage();
      }
    }

    for (const [queueId, processQueue] of this.processQueueTable.entries()) {
      if (!latest.has(queueId)) {
        processQueue.dropped = true;
        this.processQueueTable.delete(queueId);
      }
    }
  }

  getSettings(): PushSubscriptionSettings {
    return this.settings;
  }

  public wrapReceiveMessageRequest(batchSize: number, mq: MessageQueue, filterExpression: FilterExpression, invisibleDuration: number, longPollingTimeout: number): ReceiveMessageRequest {
    return super.wrapReceiveMessageRequest(batchSize, mq, filterExpression, invisibleDuration, longPollingTimeout);
  }

  public async receiveMessage(request: ReceiveMessageRequest, mq: MessageQueue, awaitDuration: number): Promise<MessageView[]> {
    return super.receiveMessage(request, mq, awaitDuration);
  }

  public async ackMessage(message: MessageView) {
    return super.ackMessage(message);
  }

    public async changeInvisibleDuration(message: MessageView, invisibleDuration: number) {
        return super.invisibleDuration(message, invisibleDuration);
    }

  public getProtobufGroup() {
    return createResource(this.consumerGroup);
  }

  wrapHeartbeatRequest(): HeartbeatRequest {
    const request = new HeartbeatRequest();
    request.setGroup(this.getProtobufGroup());
    request.setClientType(ClientType.PUSH_CONSUMER);
    return request;
  }

  wrapNotifyClientTerminationRequest(): NotifyClientTerminationRequest {
    const request = new NotifyClientTerminationRequest();
    request.setGroup(this.getProtobufGroup());
    return request;
  }
}
