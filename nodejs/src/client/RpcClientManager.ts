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

import { Metadata } from '@grpc/grpc-js';
import {
  AckMessageRequest,
  ChangeInvisibleDurationRequest,
  EndTransactionRequest,
  ForwardMessageToDeadLetterQueueRequest,
  GetOffsetRequest,
  HeartbeatRequest, NotifyClientTerminationRequest, PullMessageRequest,
  QueryAssignmentRequest, QueryOffsetRequest, QueryRouteRequest,
  ReceiveMessageRequest, SendMessageRequest, UpdateOffsetRequest,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { Endpoints } from '../route';
import { ILogger } from './Logger';
import { RpcClient } from './RpcClient';
import type { BaseClient } from './BaseClient';

const RPC_CLIENT_MAX_IDLE_DURATION = 30 * 60000; // 30 minutes
const RPC_CLIENT_IDLE_CHECK_PERIOD = 60000;

export class RpcClientManager {
  #rpcClients = new Map<Endpoints, RpcClient>();
  #baseClient: BaseClient;
  #logger: ILogger;
  #clearIdleRpcClientsTimer: NodeJS.Timeout;

  constructor(baseClient: BaseClient, logger: ILogger) {
    this.#baseClient = baseClient;
    this.#logger = logger;
    this.#startUp();
  }

  #startUp() {
    this.#clearIdleRpcClientsTimer = setInterval(() => {
      this.#clearIdleRpcClients();
    }, RPC_CLIENT_IDLE_CHECK_PERIOD);
  }

  #clearIdleRpcClients() {
    for (const [ endpoints, rpcClient ] of this.#rpcClients.entries()) {
      const idleDuration = rpcClient.idleDuration();
      if (idleDuration > RPC_CLIENT_MAX_IDLE_DURATION) {
        rpcClient.close();
        this.#rpcClients.delete(endpoints);
        this.#logger.info('[RpcClientManager] Rpc client has been idle for a long time, endpoints=%s, idleDuration=%s, clientId=%s',
          endpoints, idleDuration, RPC_CLIENT_MAX_IDLE_DURATION, this.#baseClient.clientId);
      }
    }
  }

  #getRpcClient(endpoints: Endpoints) {
    let rpcClient = this.#rpcClients.get(endpoints);
    if (!rpcClient) {
      rpcClient = new RpcClient(endpoints, this.#baseClient.sslEnabled);
      this.#rpcClients.set(endpoints, rpcClient);
    }
    return rpcClient;
  }

  close() {
    for (const [ endpoints, rpcClient ] of this.#rpcClients.entries()) {
      rpcClient.close();
      this.#rpcClients.delete(endpoints);
    }
    clearInterval(this.#clearIdleRpcClientsTimer);
  }

  async queryRoute(endpoints: Endpoints, request: QueryRouteRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.queryRoute(request, metadata, duration);
  }

  async heartbeat(endpoints: Endpoints, request: HeartbeatRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.heartbeat(request, metadata, duration);
  }

  async sendMessage(endpoints: Endpoints, request: SendMessageRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.sendMessage(request, metadata, duration);
  }

  async queryAssignment(endpoints: Endpoints, request: QueryAssignmentRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.queryAssignment(request, metadata, duration);
  }

  async receiveMessage(endpoints: Endpoints, request: ReceiveMessageRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.receiveMessage(request, metadata, duration);
  }

  async ackMessage(endpoints: Endpoints, request: AckMessageRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.ackMessage(request, metadata, duration);
  }

  async forwardMessageToDeadLetterQueue(endpoints: Endpoints, request: ForwardMessageToDeadLetterQueueRequest,
    duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.forwardMessageToDeadLetterQueue(request, metadata, duration);
  }

  async pullMessage(endpoints: Endpoints, request: PullMessageRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.pullMessage(request, metadata, duration);
  }

  async updateOffset(endpoints: Endpoints, request: UpdateOffsetRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.updateOffset(request, metadata, duration);
  }

  async getOffset(endpoints: Endpoints, request: GetOffsetRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.getOffset(request, metadata, duration);
  }

  async queryOffset(endpoints: Endpoints, request: QueryOffsetRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.queryOffset(request, metadata, duration);
  }

  async endTransaction(endpoints: Endpoints, request: EndTransactionRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.endTransaction(request, metadata, duration);
  }

  telemetry(endpoints: Endpoints, metadata: Metadata) {
    const rpcClient = this.#getRpcClient(endpoints);
    return rpcClient.telemetry(metadata);
  }

  async changeInvisibleDuration(endpoints: Endpoints, request: ChangeInvisibleDurationRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.changeInvisibleDuration(request, metadata, duration);
  }

  async notifyClientTermination(endpoints: Endpoints, request: NotifyClientTerminationRequest, duration: number) {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return await rpcClient.notifyClientTermination(request, metadata, duration);
  }
}
