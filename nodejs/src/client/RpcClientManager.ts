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
  AckMessageResponse,
  ChangeInvisibleDurationRequest,
  ChangeInvisibleDurationResponse,
  EndTransactionRequest,
  EndTransactionResponse,
  ForwardMessageToDeadLetterQueueRequest,
  ForwardMessageToDeadLetterQueueResponse,
  GetOffsetRequest,
  GetOffsetResponse,
  HeartbeatRequest,
  HeartbeatResponse,
  NotifyClientTerminationRequest,
  NotifyClientTerminationResponse,
  PullMessageRequest,
  PullMessageResponse,
  QueryAssignmentRequest,
  QueryAssignmentResponse,
  QueryOffsetRequest,
  QueryOffsetResponse,
  QueryRouteRequest,
  QueryRouteResponse,
  ReceiveMessageRequest,
  ReceiveMessageResponse,
  SendMessageRequest,
  SendMessageResponse,
  UpdateOffsetRequest,
  UpdateOffsetResponse,
  RecallMessageRequest,
  RecallMessageResponse,
  SyncLiteSubscriptionRequest,
  SyncLiteSubscriptionResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { Endpoints } from '../route';
import { withRequestId } from '../util/rpcContext';
import { ILogger } from './Logger';
import { RpcClient } from './RpcClient';
import type { BaseClient } from './BaseClient';

const RPC_CLIENT_MAX_IDLE_DURATION = 30 * 60000; // 30 minutes
const RPC_CLIENT_IDLE_CHECK_PERIOD = 60000;

export class RpcClientManager {
  // Keyed by endpoints.facade (string) instead of the Endpoints object itself:
  // Endpoints instances are recreated on every route fetch, so object keys would
  // leak duplicate RpcClients for logically identical endpoints.
  #rpcClients = new Map<string, RpcClient>();
  #baseClient: BaseClient;
  #logger: ILogger;
  #clearIdleRpcClientsTimer: NodeJS.Timeout;
  // Terminal state set by close(): afterwards no RPC client may be recreated,
  // otherwise residual callbacks (e.g. a receive loop that escaped shutdown)
  // would lazily rebuild gRPC channels and keep hitting the remote.
  #closed = false;

  constructor(baseClient: BaseClient, logger: ILogger) {
    this.#baseClient = baseClient;
    this.#logger = logger;
    this.#startUp();
  }

  #startUp() {
    this.#clearIdleRpcClientsTimer = setInterval(() => {
      this.#clearIdleRpcClients();
    }, RPC_CLIENT_IDLE_CHECK_PERIOD);
    // Housekeeping only: a client that is constructed but never started must
    // not keep the process alive.
    this.#clearIdleRpcClientsTimer.unref();
  }

  #clearIdleRpcClients() {
    for (const [ facade, rpcClient ] of this.#rpcClients.entries()) {
      const idleDuration = rpcClient.idleDuration();
      if (idleDuration > RPC_CLIENT_MAX_IDLE_DURATION) {
        rpcClient.close();
        this.#rpcClients.delete(facade);
        this.#logger.info('[RpcClientManager] Rpc client has been idle for a long time, endpoints=%s, idleDuration=%dms, maxIdleDuration=%dms, clientId=%s',
          facade, idleDuration, RPC_CLIENT_MAX_IDLE_DURATION, this.#baseClient.clientId);
      }
    }
  }

  #getRpcClient(endpoints: Endpoints) {
    if (this.#closed) {
      throw new Error(`RpcClientManager is already closed, will not re-establish connection, endpoints=${endpoints.facade}, clientId=${this.#baseClient.clientId}`);
    }
    const facade = endpoints.facade;
    let rpcClient = this.#rpcClients.get(facade);
    if (!rpcClient) {
      rpcClient = new RpcClient(endpoints, this.#baseClient.sslEnabled);
      this.#rpcClients.set(facade, rpcClient);
    }
    return rpcClient;
  }

  /**
   * Close and remove the RPC client bound to the given endpoints (e.g. after
   * consecutive heartbeat failures), so a fresh channel is established on next use.
   */
  evict(endpoints: Endpoints) {
    const facade = endpoints.facade;
    const rpcClient = this.#rpcClients.get(facade);
    if (rpcClient) {
      rpcClient.close();
      this.#rpcClients.delete(facade);
      this.#logger.info('[RpcClientManager] Rpc client evicted, endpoints=%s, clientId=%s',
        facade, this.#baseClient.clientId);
    }
  }

  close() {
    // Mark closed first so no RPC client can be lazily recreated afterwards
    this.#closed = true;

    // Clear idle check timer first
    if (this.#clearIdleRpcClientsTimer) {
      clearInterval(this.#clearIdleRpcClientsTimer);
    }

    // Close all RPC clients and clear the map
    for (const [ facade, rpcClient ] of this.#rpcClients.entries()) {
      try {
        rpcClient.close();
      } catch (e) {
        this.#logger.warn('Failed to close RPC client for endpoints=%s, clientId=%s, error=%s',
          facade, this.#baseClient.clientId, e instanceof Error ? e.message : String(e));
      }
    }
    this.#rpcClients.clear();
  }

  // Shared plumbing for unary calls: resolve the channel, build request metadata
  // and associate the response with its request id.
  async #call<T extends object>(endpoints: Endpoints,
    invoke: (rpcClient: RpcClient, metadata: Metadata) => Promise<T>): Promise<T> {
    const rpcClient = this.#getRpcClient(endpoints);
    const metadata = this.#baseClient.getRequestMetadata();
    return withRequestId(await invoke(rpcClient, metadata), metadata);
  }

  async queryRoute(endpoints: Endpoints, request: QueryRouteRequest, duration: number) {
    return this.#call<QueryRouteResponse>(endpoints, (client, metadata) => client.queryRoute(request, metadata, duration));
  }

  async heartbeat(endpoints: Endpoints, request: HeartbeatRequest, duration: number) {
    return this.#call<HeartbeatResponse>(endpoints, (client, metadata) => client.heartbeat(request, metadata, duration));
  }

  async sendMessage(endpoints: Endpoints, request: SendMessageRequest, duration: number) {
    return this.#call<SendMessageResponse>(endpoints, (client, metadata) => client.sendMessage(request, metadata, duration));
  }

  async queryAssignment(endpoints: Endpoints, request: QueryAssignmentRequest, duration: number) {
    return this.#call<QueryAssignmentResponse>(endpoints,
      (client, metadata) => client.queryAssignment(request, metadata, duration));
  }

  async receiveMessage(endpoints: Endpoints, request: ReceiveMessageRequest, duration: number) {
    return this.#call<ReceiveMessageResponse[]>(endpoints,
      (client, metadata) => client.receiveMessage(request, metadata, duration));
  }

  async ackMessage(endpoints: Endpoints, request: AckMessageRequest, duration: number) {
    return this.#call<AckMessageResponse>(endpoints, (client, metadata) => client.ackMessage(request, metadata, duration));
  }

  async forwardMessageToDeadLetterQueue(endpoints: Endpoints, request: ForwardMessageToDeadLetterQueueRequest,
    duration: number) {
    return this.#call<ForwardMessageToDeadLetterQueueResponse>(endpoints,
      (client, metadata) => client.forwardMessageToDeadLetterQueue(request, metadata, duration));
  }

  async pullMessage(endpoints: Endpoints, request: PullMessageRequest, duration: number) {
    return this.#call<PullMessageResponse[]>(endpoints,
      (client, metadata) => client.pullMessage(request, metadata, duration));
  }

  async updateOffset(endpoints: Endpoints, request: UpdateOffsetRequest, duration: number) {
    return this.#call<UpdateOffsetResponse>(endpoints,
      (client, metadata) => client.updateOffset(request, metadata, duration));
  }

  async getOffset(endpoints: Endpoints, request: GetOffsetRequest, duration: number) {
    return this.#call<GetOffsetResponse>(endpoints, (client, metadata) => client.getOffset(request, metadata, duration));
  }

  async queryOffset(endpoints: Endpoints, request: QueryOffsetRequest, duration: number) {
    return this.#call<QueryOffsetResponse>(endpoints,
      (client, metadata) => client.queryOffset(request, metadata, duration));
  }

  async endTransaction(endpoints: Endpoints, request: EndTransactionRequest, duration: number) {
    return this.#call<EndTransactionResponse>(endpoints,
      (client, metadata) => client.endTransaction(request, metadata, duration));
  }

  telemetry(endpoints: Endpoints, metadata: Metadata) {
    const rpcClient = this.#getRpcClient(endpoints);
    return withRequestId(rpcClient.telemetry(metadata), metadata);
  }

  async changeInvisibleDuration(endpoints: Endpoints, request: ChangeInvisibleDurationRequest, duration: number) {
    return this.#call<ChangeInvisibleDurationResponse>(endpoints,
      (client, metadata) => client.changeInvisibleDuration(request, metadata, duration));
  }

  async notifyClientTermination(endpoints: Endpoints, request: NotifyClientTerminationRequest, duration: number) {
    return this.#call<NotifyClientTerminationResponse>(endpoints,
      (client, metadata) => client.notifyClientTermination(request, metadata, duration));
  }

  async recallMessage(endpoints: Endpoints, request: RecallMessageRequest, duration: number) {
    return this.#call<RecallMessageResponse>(endpoints,
      (client, metadata) => client.recallMessage(request, metadata, duration));
  }

  async syncLiteSubscription(endpoints: Endpoints, request: SyncLiteSubscriptionRequest, duration: number) {
    return this.#call<SyncLiteSubscriptionResponse>(endpoints,
      (client, metadata) => client.syncLiteSubscription(request, metadata, duration));
  }
}
