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

import { ClientType, MessageType, Permission } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  AckMessageRequest,
  ChangeInvisibleDurationRequest,
  ForwardMessageToDeadLetterQueueRequest,
  HeartbeatRequest,
  NotifyClientTerminationRequest,
  QueryAssignmentRequest,
  ReceiveMessageRequest,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { MessageView } from '../message';
import { Broker, MessageQueue, TopicRouteData } from '../route';
import { MessageQueue as MessageQueuePB, Broker as BrokerPB } from '../../proto/apache/rocketmq/v2/definition_pb';
import { StatusChecker } from '../exception';
import { RetryPolicy } from '../retry';
import { createDuration, createResource } from '../util';
import { Consumer, ConsumerOptions } from './Consumer';
import { FilterExpression } from './FilterExpression';
import { PushSubscriptionSettings } from './PushSubscriptionSettings';
import { ConsumeService } from './ConsumeService';
import { StandardConsumeService } from './StandardConsumeService';
import { FifoConsumeService } from './FifoConsumeService';
import { ProcessQueue } from './ProcessQueue';
import { Assignment } from './Assignment';
import { Assignments } from './Assignments';
import { MessageListener } from './MessageListener';

const ASSIGNMENT_SCAN_SCHEDULE_DELAY = 1000;
const ASSIGNMENT_SCAN_SCHEDULE_PERIOD = 5000;

export interface PushConsumerOptions extends ConsumerOptions {
  subscriptions: Map<string/* topic */, FilterExpression | string>;
  messageListener: MessageListener;
  maxCacheMessageCount?: number;
  maxCacheMessageSizeInBytes?: number;
  longPollingTimeout?: number;
  enableFifoConsumeAccelerator?: boolean;
}

class ConsumeMetrics {
  receptionTimes = 0;
  receivedMessagesQuantity = 0;
  consumptionOkQuantity = 0;
  consumptionErrorQuantity = 0;
}

export class PushConsumer extends Consumer {
  readonly #pushSubscriptionSettings: PushSubscriptionSettings;
  readonly #subscriptionExpressions = new Map<string, FilterExpression>();
  readonly #cacheAssignments = new Map<string, Assignments>();
  readonly #messageListener: MessageListener;
  readonly #maxCacheMessageCount: number;
  readonly #maxCacheMessageSizeInBytes: number;
  readonly #enableFifoConsumeAccelerator: boolean;
  readonly #processQueueTable = new Map<string /* mq key */, { mq: MessageQueue; pq: ProcessQueue }>();
  readonly #metrics = new ConsumeMetrics();
  #consumeService!: ConsumeService;
  #scanAssignmentTimer?: NodeJS.Timeout;

  constructor(options: PushConsumerOptions) {
    options.topics = Array.from(options.subscriptions.keys());
    super(options);

    for (const [ topic, filter ] of options.subscriptions.entries()) {
      if (typeof filter === 'string') {
        this.#subscriptionExpressions.set(topic, new FilterExpression(filter));
      } else {
        this.#subscriptionExpressions.set(topic, filter);
      }
    }

    this.#messageListener = options.messageListener;
    this.#maxCacheMessageCount = options.maxCacheMessageCount ?? 1024;
    this.#maxCacheMessageSizeInBytes = options.maxCacheMessageSizeInBytes ?? 64 * 1024 * 1024;
    this.#enableFifoConsumeAccelerator = options.enableFifoConsumeAccelerator ?? false;

    this.#pushSubscriptionSettings = new PushSubscriptionSettings(
      options.namespace, this.clientId, this.getClientType(), this.endpoints,
      this.consumerGroup, this.requestTimeout, this.#subscriptionExpressions,
    );
  }

  async startup() {
    this.logger.info('Begin to start the rocketmq push consumer, clientId=%s, consumerGroup=%s',
      this.clientId, this.consumerGroup);
    await super.startup();
    this.logger.info('Super startup completed, clientId=%s', this.clientId);
    try {
      this.#consumeService = this.#createConsumeService();
      // Start scanning assignments periodically
      this.startAssignmentScanning();
      this.logger.info('Push consumer started successfully, clientId=%s', this.clientId);
    } catch (err) {
      this.logger.error('Failed to start push consumer, cleaning up resources, clientId=%s, error=%s',
        this.clientId, err);
      // Clean up timers if initialization fails
      if (this.#scanAssignmentTimer) {
        clearInterval(this.#scanAssignmentTimer);
        this.#scanAssignmentTimer = undefined;
      }
      throw err;
    }
  }

  /**
   * Start assignment scanning. Can be overridden by subclasses.
   */
  protected startAssignmentScanning() {
    this.logger.info('Starting assignment scanning, clientId=%s', this.clientId);
    setTimeout(() => this.#scanAssignments(), ASSIGNMENT_SCAN_SCHEDULE_DELAY);
    this.#scanAssignmentTimer = setInterval(() => this.#scanAssignments(), ASSIGNMENT_SCAN_SCHEDULE_PERIOD);
  }

  async shutdown() {
    this.logger.info('Begin to shutdown the rocketmq push consumer, clientId=%s', this.clientId);
    // Stop scanning assignments
    if (this.#scanAssignmentTimer) {
      clearInterval(this.#scanAssignmentTimer);
      this.#scanAssignmentTimer = undefined;
    }
    // Drop all process queues
    this.logger.info('Dropping all process queues, clientId=%s, queueCount=%d',
      this.clientId, this.#processQueueTable.size);
    for (const { pq } of this.#processQueueTable.values()) {
      pq.drop();
      pq.abort();
    }
    this.#processQueueTable.clear();
    // Shutdown consume service
    if (this.#consumeService) {
      this.logger.info('Shutting down consume service, clientId=%s', this.clientId);
      this.#consumeService.abort();
    }
    await super.shutdown();
    this.logger.info('Push consumer has been shutdown successfully, clientId=%s', this.clientId);
  }

  protected getSettings() {
    return this.#pushSubscriptionSettings;
  }

  /**
   * Get the client type for push consumer.
   *
   * @return The client type identifier
   */
  protected getClientType(): ClientType {
    return ClientType.PUSH_CONSUMER;
  }

  protected wrapHeartbeatRequest() {
    return new HeartbeatRequest()
      .setClientType(this.getClientType())
      .setGroup(createResource(this.consumerGroup));
  }

  protected wrapNotifyClientTerminationRequest() {
    return new NotifyClientTerminationRequest()
      .setGroup(createResource(this.consumerGroup));
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected onTopicRouteDataUpdate(_topic: string, _topicRouteData: TopicRouteData) {
    // No-op for push consumer; assignments are queried separately
  }

  #createConsumeService(): ConsumeService {
    if (this.#pushSubscriptionSettings.isFifo()) {
      return new FifoConsumeService(this.clientId, this.#messageListener, this.#enableFifoConsumeAccelerator);
    }
    return new StandardConsumeService(this.clientId, this.#messageListener);
  }

  async subscribe(topic: string, filterExpression: FilterExpression) {
    await this.getRouteData(topic);
    this.#subscriptionExpressions.set(topic, filterExpression);
  }

  unsubscribe(topic: string) {
    this.#subscriptionExpressions.delete(topic);
  }

  // --- Public methods for ProcessQueue access ---

  get requestTimeoutValue(): number {
    return this.requestTimeout;
  }

  getPushConsumerSettings(): PushSubscriptionSettings {
    return this.#pushSubscriptionSettings;
  }

  getConsumerGroup(): string {
    return this.consumerGroup;
  }

  getConsumeService(): ConsumeService {
    return this.#consumeService;
  }

  getRetryPolicy(): RetryPolicy | undefined {
    return this.#pushSubscriptionSettings.getRetryPolicy();
  }

  getClientId(): string {
    return this.clientId;
  }

  // --- Metrics access ---

  getReceptionTimes(): number {
    return this.#metrics.receptionTimes;
  }

  getReceivedMessagesQuantity(): number {
    return this.#metrics.receivedMessagesQuantity;
  }

  getConsumptionOkQuantity(): number {
    return this.#metrics.consumptionOkQuantity;
  }

  getConsumptionErrorQuantity(): number {
    return this.#metrics.consumptionErrorQuantity;
  }

  incrementReceptionTimes() {
    this.#metrics.receptionTimes++;
  }

  incrementReceivedMessagesQuantity(count: number) {
    this.#metrics.receivedMessagesQuantity += count;
  }

  incrementConsumptionOkQuantity() {
    this.#metrics.consumptionOkQuantity++;
  }

  incrementConsumptionErrorQuantity() {
    this.#metrics.consumptionErrorQuantity++;
  }

  // --- Statistics ---

  doStats() {
    const receptionTimes = this.#metrics.receptionTimes;
    this.#metrics.receptionTimes = 0;
    const receivedMessagesQuantity = this.#metrics.receivedMessagesQuantity;
    this.#metrics.receivedMessagesQuantity = 0;
    const consumptionOkQuantity = this.#metrics.consumptionOkQuantity;
    this.#metrics.consumptionOkQuantity = 0;
    const consumptionErrorQuantity = this.#metrics.consumptionErrorQuantity;
    this.#metrics.consumptionErrorQuantity = 0;

    this.logger.info('clientId=%s, consumerGroup=%s, receptionTimes=%d, receivedMessagesQuantity=%d, '
      + 'consumptionOkQuantity=%d, consumptionErrorQuantity=%d',
    this.clientId, this.consumerGroup, receptionTimes, receivedMessagesQuantity,
    consumptionOkQuantity, consumptionErrorQuantity);
  }

  wrapPushReceiveMessageRequest(
    batchSize: number,
    mq: MessageQueue,
    filterExpression: FilterExpression,
    longPollingTimeout: number,
    attemptId?: string,
  ) {
    const request = new ReceiveMessageRequest()
      .setGroup(createResource(this.consumerGroup))
      .setMessageQueue(mq.toProtobuf())
      .setFilterExpression(filterExpression.toProtobuf())
      .setLongPollingTimeout(createDuration(longPollingTimeout))
      .setBatchSize(batchSize)
      .setAutoRenew(true);
    if (attemptId) {
      request.setAttemptId(attemptId);
    }
    return request;
  }

  async receiveMessage(request: ReceiveMessageRequest, mq: MessageQueue, awaitDuration: number) {
    return super.receiveMessage(request, mq, awaitDuration);
  }

  wrapAckMessageRequest(messageView: MessageView) {
    const request = new AckMessageRequest()
      .setGroup(createResource(this.consumerGroup))
      .setTopic(createResource(messageView.topic));
    request.addEntries()
      .setMessageId(messageView.messageId)
      .setReceiptHandle(messageView.receiptHandle);
    return request;
  }

  wrapChangeInvisibleDurationRequest(messageView: MessageView, invisibleDuration: number) {
    return new ChangeInvisibleDurationRequest()
      .setGroup(createResource(this.consumerGroup))
      .setTopic(createResource(messageView.topic))
      .setReceiptHandle(messageView.receiptHandle)
      .setInvisibleDuration(createDuration(invisibleDuration))
      .setMessageId(messageView.messageId);
  }

  wrapForwardMessageToDeadLetterQueueRequest(messageView: MessageView) {
    const retryPolicy = this.getRetryPolicy();
    return new ForwardMessageToDeadLetterQueueRequest()
      .setGroup(createResource(this.consumerGroup))
      .setTopic(createResource(messageView.topic))
      .setReceiptHandle(messageView.receiptHandle)
      .setMessageId(messageView.messageId)
      .setDeliveryAttempt(messageView.deliveryAttempt ?? 0)
      .setMaxDeliveryAttempts(retryPolicy?.getMaxAttempts() ?? 1);
  }

  // --- Internal: queue size and cache threshold ---

  getQueueSize(): number {
    return this.#processQueueTable.size;
  }

  cacheMessageBytesThresholdPerQueue(): number {
    const size = this.getQueueSize();
    if (size <= 0) return 0;
    return Math.max(1, Math.floor(this.#maxCacheMessageSizeInBytes / size));
  }

  cacheMessageCountThresholdPerQueue(): number {
    const size = this.getQueueSize();
    if (size <= 0) return 0;
    return Math.max(1, Math.floor(this.#maxCacheMessageCount / size));
  }

  // --- Internal: assignment scanning ---

  #scanAssignments() {
    try {
      this.logger.debug?.('Scanning assignments, clientId=%s, subscriptionCount=%d',
        this.clientId, this.#subscriptionExpressions.size);
      for (const [ topic, filterExpression ] of this.#subscriptionExpressions) {
        const existed = this.#cacheAssignments.get(topic);
        this.#queryAssignment(topic)
          .then(latest => {
            this.logger.debug?.('Query assignment result, topic=%s, clientId=%s, assignmentCount=%d',
              topic, this.clientId, latest.getAssignmentList().length);
            if (latest.getAssignmentList().length === 0) {
              if (!existed || existed.getAssignmentList().length === 0) {
                this.logger.info('Acquired empty assignments from remote, would scan later, topic=%s, '
                  + 'clientId=%s', topic, this.clientId);
                return;
              }
              this.logger.warn('Attention!!! acquired empty assignments from remote, but existed assignments'
                + ' is not empty, topic=%s, clientId=%s', topic, this.clientId);
            }

            if (!latest.equals(existed)) {
              this.logger.info('Assignments of topic=%s has changed, %j => %j, clientId=%s', topic, existed,
                latest, this.clientId);
              this.#syncProcessQueue(topic, latest, filterExpression);
              this.#cacheAssignments.set(topic, latest);
              return;
            }
            this.logger.debug?.('Assignments of topic=%s remains the same, assignments=%j, clientId=%s', topic,
              existed, this.clientId);
            // Process queue may be dropped, need to be synchronized anyway.
            this.#syncProcessQueue(topic, latest, filterExpression);
          })
          .catch(err => {
            this.logger.error('Exception raised while scanning the assignments, topic=%s, clientId=%s, error=%s',
              topic, this.clientId, err);
          });
      }
    } catch (err) {
      this.logger.error('Exception raised while scanning the assignments for all topics, clientId=%s, error=%s',
        this.clientId, err);
    }
  }

  async #queryAssignment(topic: string): Promise<Assignments> {
    const topicRouteData = await this.getRouteData(topic);
    const endpointsList = topicRouteData.getTotalEndpoints();
    if (endpointsList.length === 0) {
      throw new Error(`No endpoints available for topic=${topic}`);
    }
    const endpoints = endpointsList[0];

    const request = new QueryAssignmentRequest()
      .setTopic(createResource(topic))
      .setGroup(createResource(this.consumerGroup))
      .setEndpoints(this.endpoints.toProtobuf());

    this.logger.info('Querying assignment, topic=%s, consumerGroup=%s, clientId=%s',
      topic, this.consumerGroup, this.clientId);

    const response = await this.rpcClientManager.queryAssignment(
      endpoints, request, this.requestTimeout,
    );
    const status = response.getStatus();
    if (!status) {
      throw new Error('Missing status in query assignment response');
    }
    StatusChecker.check(status.toObject());

    const assignmentList = response.getAssignmentsList().map(assignment => {
      const mqPb = assignment.getMessageQueue()!;
      const mq = new MessageQueue(mqPb);
      this.logger.info('Assignment received: topic=%s, broker=%s, queueId=%d, clientId=%s',
        mq.topic.name, mq.broker.name, mq.queueId, this.clientId);
      return new Assignment(mq);
    });

    this.logger.info('Total assignments: %d, topic=%s, clientId=%s',
      assignmentList.length, topic, this.clientId);

    return new Assignments(assignmentList);
  }

  #syncProcessQueue(topic: string, assignments: Assignments, filterExpression: FilterExpression) {
    const latestMqKeys = new Set<string>();
    for (const assignment of assignments.getAssignmentList()) {
      latestMqKeys.add(this.#mqKey(assignment.messageQueue));
    }

    // Find active message queues
    const activeMqKeys = new Set<string>();
    for (const [ mqKey, { mq, pq }] of this.#processQueueTable) {
      if (mq.topic.name !== topic) {
        continue;
      }
      if (!latestMqKeys.has(mqKey)) {
        this.logger.info('Drop message queue according to the latest assignmentList, mq=%s@%s@%s, clientId=%s',
          mq.topic.name, mq.broker.name, mq.queueId, this.clientId);
        this.#dropProcessQueue(mqKey);
        continue;
      }
      if (pq.expired()) {
        this.logger.warn('Drop message queue because it is expired, mq=%s@%s@%s, clientId=%s',
          mq.topic.name, mq.broker.name, mq.queueId, this.clientId);
        this.#dropProcessQueue(mqKey);
        continue;
      }
      activeMqKeys.add(mqKey);
    }

    // Create new process queues for new assignments
    for (const assignment of assignments.getAssignmentList()) {
      const mqKey = this.#mqKey(assignment.messageQueue);
      if (activeMqKeys.has(mqKey)) {
        continue;
      }
      const processQueue = this.#createProcessQueue(assignment.messageQueue, filterExpression);
      if (processQueue) {
        this.logger.info('Start to fetch message from remote, mq=%s@%s@%s, clientId=%s',
          assignment.messageQueue.topic.name, assignment.messageQueue.broker.name,
          assignment.messageQueue.queueId, this.clientId);
        processQueue.fetchMessageImmediately();
      }
    }
  }

  #mqKey(mq: MessageQueue): string {
    return `${mq.topic.name}@${mq.broker.name}@${mq.queueId}`;
  }

  #dropProcessQueue(mqKey: string) {
    const entry = this.#processQueueTable.get(mqKey);
    if (entry) {
      entry.pq.drop();
      this.#processQueueTable.delete(mqKey);
    }
  }

  #createProcessQueue(mq: MessageQueue, filterExpression: FilterExpression): ProcessQueue | null {
    const mqKey = this.#mqKey(mq);
    if (this.#processQueueTable.has(mqKey)) {
      return null;
    }
    const processQueue = new ProcessQueue(this, mq, filterExpression);
    this.#processQueueTable.set(mqKey, { mq, pq: processQueue });
    return processQueue;
  }

  /**
   * Create a virtual process queue for Lite consumer.
   * Lite consumers use a special message queue with queueId=-1 to receive messages.
   * This method creates the virtual queue and starts fetching messages immediately.
   *
   * @param topic - The bind topic name
   * @param filterExpression - The filter expression for message filtering
   */
  protected createVirtualProcessQueueForLite(topic: string, filterExpression: FilterExpression) {
    // Validate parameters
    if (!topic || topic.trim().length === 0) {
      throw new Error('topic should not be blank');
    }
    if (!filterExpression) {
      throw new Error('filterExpression should not be null');
    }

    // Create a virtual broker with default endpoints
    const brokerPb = new BrokerPB();
    brokerPb.setName('virtual-broker');
    brokerPb.setId(0);
    brokerPb.setEndpoints(this.endpoints.toProtobuf());
    const broker = new Broker(brokerPb.toObject());

    // Create a virtual message queue with queueId=-1
    const virtualMqPb = new MessageQueuePB();
    virtualMqPb.setId(-1); // Virtual queue ID for Lite consumer
    virtualMqPb.setTopic(createResource(topic));
    virtualMqPb.setBroker(broker.toProtobuf());
    virtualMqPb.setPermission(Permission.READ);
    virtualMqPb.setAcceptMessageTypesList([ MessageType.LITE ]);

    const virtualMq = new MessageQueue(virtualMqPb);

    // Create process queue for the virtual message queue
    const mqKey = this.#mqKey(virtualMq);
    if (this.#processQueueTable.has(mqKey)) {
      this.logger.info('Virtual process queue already exists, mq=%s, clientId=%s', mqKey, this.clientId);
      return;
    }

    const processQueue = new ProcessQueue(this, virtualMq, filterExpression);
    this.#processQueueTable.set(mqKey, { mq: virtualMq, pq: processQueue });

    this.logger.info('Created virtual process queue for Lite consumer, mq=%s, clientId=%s',
      mqKey, this.clientId);

    // Start fetching messages immediately
    this.logger.info('Start to fetch lite messages from virtual queue, mq=%s, clientId=%s',
      mqKey, this.clientId);
    processQueue.fetchMessageImmediately();
  }
}
