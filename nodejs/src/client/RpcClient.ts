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

import { ChannelCredentials, Metadata } from '@grpc/grpc-js';
import { MessagingServiceClient } from '../../proto/apache/rocketmq/v2/service_grpc_pb';
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
} from '../../proto/apache/rocketmq/v2/service_pb';
import { Endpoints } from '../route';

export class RpcClient {
  #client: MessagingServiceClient;
  #activityTime = Date.now();

  constructor(endpoints: Endpoints, sslEnabled: boolean) {
    const address = endpoints.getGrpcTarget();
    const grpcCredentials = sslEnabled ? ChannelCredentials.createSsl() : ChannelCredentials.createInsecure();
    this.#client = new MessagingServiceClient(address, grpcCredentials);
  }

  #getAndActivityRpcClient() {
    this.#activityTime = Date.now();
    return this.#client;
  }

  #getDeadline(duration: number) {
    return Date.now() + duration;
  }

  idleDuration() {
    return Date.now() - this.#activityTime;
  }

  close() {
    this.#client.close();
  }

  /**
   * Query topic route
   *
   * @param request query route request.
   * @param metadata gRPC request header metadata.
   * @param duration request max duration in milliseconds.
   */
  async queryRoute(request: QueryRouteRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    return new Promise<QueryRouteResponse>((resolve, reject) => {
      client.queryRoute(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }

  async heartbeat(request: HeartbeatRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    return new Promise<HeartbeatResponse>((resolve, reject) => {
      client.heartbeat(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }

  async sendMessage(request: SendMessageRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    return new Promise<SendMessageResponse>((resolve, reject) => {
      client.sendMessage(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }

  async queryAssignment(request: QueryAssignmentRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    return new Promise<QueryAssignmentResponse>((resolve, reject) => {
      client.queryAssignment(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }

  async receiveMessage(request: ReceiveMessageRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    const readable = client.receiveMessage(request, metadata, { deadline });
    const responses: ReceiveMessageResponse[] = [];
    for await (const res of readable) {
      responses.push(res);
    }
    return responses;
  }

  async ackMessage(request: AckMessageRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    return new Promise<AckMessageResponse>((resolve, reject) => {
      client.ackMessage(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }

  async forwardMessageToDeadLetterQueue(request: ForwardMessageToDeadLetterQueueRequest,
    metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    return new Promise<ForwardMessageToDeadLetterQueueResponse>((resolve, reject) => {
      client.forwardMessageToDeadLetterQueue(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }

  async pullMessage(request: PullMessageRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    const readable = client.pullMessage(request, metadata, { deadline });
    const responses: PullMessageResponse[] = [];
    for await (const res of readable) {
      responses.push(res);
    }
    return responses;
  }

  async updateOffset(request: UpdateOffsetRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    return new Promise<UpdateOffsetResponse>((resolve, reject) => {
      client.updateOffset(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }

  async getOffset(request: GetOffsetRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    return new Promise<GetOffsetResponse>((resolve, reject) => {
      client.getOffset(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }

  async queryOffset(request: QueryOffsetRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    return new Promise<QueryOffsetResponse>((resolve, reject) => {
      client.queryOffset(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }

  async endTransaction(request: EndTransactionRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    return new Promise<EndTransactionResponse>((resolve, reject) => {
      client.endTransaction(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }

  telemetry(metadata: Metadata) {
    const client = this.#getAndActivityRpcClient();
    return client.telemetry(metadata);
  }

  async notifyClientTermination(request: NotifyClientTerminationRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    return new Promise<NotifyClientTerminationResponse>((resolve, reject) => {
      client.notifyClientTermination(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }

  async changeInvisibleDuration(request: ChangeInvisibleDurationRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const deadline = this.#getDeadline(duration);
    return new Promise<ChangeInvisibleDurationResponse>((resolve, reject) => {
      client.changeInvisibleDuration(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
  }
}
