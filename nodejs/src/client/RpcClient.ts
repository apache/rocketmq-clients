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
  RecallMessageRequest,
  RecallMessageResponse,
  ReceiveMessageRequest,
  ReceiveMessageResponse,
  SendMessageRequest,
  SendMessageResponse,
  SyncLiteSubscriptionRequest,
  SyncLiteSubscriptionResponse,
  UpdateOffsetRequest,
  UpdateOffsetResponse,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { Endpoints } from '../route';

// Channel options mirroring the Java client's Netty channel configuration:
// keepalive probing plus unbounded message sizes (Java defaults to Integer.MAX_VALUE).
const GRPC_CHANNEL_OPTIONS = {
  'grpc.keepalive_time_ms': 300000, // 5 minutes, same as Java DEFAULT_KEEP_ALIVE_TIME_MILLIS
  'grpc.keepalive_timeout_ms': 30000, // 30 seconds, same as Java DEFAULT_KEEP_ALIVE_TIMEOUT_MILLIS
  'grpc.keepalive_permit_without_calls': 1,
  'grpc.max_send_message_length': 2 ** 31 - 1,
  'grpc.max_receive_message_length': 2 ** 31 - 1,
  // Use a local subchannel pool so each RpcClient owns its own connection
  // instead of sharing one via grpc-js's global pool, aligning with the Java client.
  'grpc.use_local_subchannel_pool': 1,
};

export class RpcClient {
  #client: MessagingServiceClient;
  #activityTime = Date.now();

  constructor(endpoints: Endpoints, sslEnabled: boolean) {
    const address = endpoints.getGrpcTarget();
    const grpcCredentials = sslEnabled ? ChannelCredentials.createSsl() : ChannelCredentials.createInsecure();
    this.#client = new MessagingServiceClient(address, grpcCredentials, GRPC_CHANNEL_OPTIONS);
  }

  #getAndActivityRpcClient() {
    this.#activityTime = Date.now();
    return this.#client;
  }

  // Shared unary-call plumbing: refresh activity, apply the deadline and settle
  // the promise from the gRPC callback, mirroring the per-method wrappers before.
  #unary<Req, Res>(duration: number,
    call: (request: Req, metadata: Metadata, options: { deadline: number },
      callback: (e: Error | null, res: Res) => void) => void,
    request: Req, metadata: Metadata): Promise<Res> {
    this.#activityTime = Date.now();
    const deadline = Date.now() + duration;
    return new Promise<Res>((resolve, reject) => {
      call(request, metadata, { deadline }, (e, res) => {
        if (e) return reject(e);
        resolve(res);
      });
    });
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
  async queryRoute(request: QueryRouteRequest, metadata: Metadata, duration: number): Promise<QueryRouteResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.queryRoute(req, md, opts, cb), request, metadata);
  }

  async heartbeat(request: HeartbeatRequest, metadata: Metadata, duration: number): Promise<HeartbeatResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.heartbeat(req, md, opts, cb), request, metadata);
  }

  async sendMessage(request: SendMessageRequest, metadata: Metadata, duration: number): Promise<SendMessageResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.sendMessage(req, md, opts, cb), request, metadata);
  }

  async queryAssignment(request: QueryAssignmentRequest, metadata: Metadata, duration: number): Promise<QueryAssignmentResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.queryAssignment(req, md, opts, cb), request, metadata);
  }

  async receiveMessage(request: ReceiveMessageRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const readable = client.receiveMessage(request, metadata, { deadline: Date.now() + duration });
    const responses: ReceiveMessageResponse[] = [];
    for await (const res of readable) {
      responses.push(res);
    }
    return responses;
  }

  async ackMessage(request: AckMessageRequest, metadata: Metadata, duration: number): Promise<AckMessageResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.ackMessage(req, md, opts, cb), request, metadata);
  }

  async forwardMessageToDeadLetterQueue(request: ForwardMessageToDeadLetterQueueRequest,
    metadata: Metadata, duration: number): Promise<ForwardMessageToDeadLetterQueueResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.forwardMessageToDeadLetterQueue(req, md, opts, cb),
      request, metadata);
  }

  async pullMessage(request: PullMessageRequest, metadata: Metadata, duration: number) {
    const client = this.#getAndActivityRpcClient();
    const readable = client.pullMessage(request, metadata, { deadline: Date.now() + duration });
    const responses: PullMessageResponse[] = [];
    for await (const res of readable) {
      responses.push(res);
    }
    return responses;
  }

  async updateOffset(request: UpdateOffsetRequest, metadata: Metadata, duration: number): Promise<UpdateOffsetResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.updateOffset(req, md, opts, cb), request, metadata);
  }

  async getOffset(request: GetOffsetRequest, metadata: Metadata, duration: number): Promise<GetOffsetResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.getOffset(req, md, opts, cb), request, metadata);
  }

  async queryOffset(request: QueryOffsetRequest, metadata: Metadata, duration: number): Promise<QueryOffsetResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.queryOffset(req, md, opts, cb), request, metadata);
  }

  async endTransaction(request: EndTransactionRequest, metadata: Metadata, duration: number): Promise<EndTransactionResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.endTransaction(req, md, opts, cb), request, metadata);
  }

  telemetry(metadata: Metadata) {
    const client = this.#getAndActivityRpcClient();
    return client.telemetry(metadata);
  }

  async notifyClientTermination(request: NotifyClientTerminationRequest, metadata: Metadata, duration: number): Promise<NotifyClientTerminationResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.notifyClientTermination(req, md, opts, cb),
      request, metadata);
  }

  async changeInvisibleDuration(request: ChangeInvisibleDurationRequest, metadata: Metadata, duration: number): Promise<ChangeInvisibleDurationResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.changeInvisibleDuration(req, md, opts, cb),
      request, metadata);
  }

  async recallMessage(request: RecallMessageRequest, metadata: Metadata, duration: number): Promise<RecallMessageResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.recallMessage(req, md, opts, cb), request, metadata);
  }

  async syncLiteSubscription(request: SyncLiteSubscriptionRequest, metadata: Metadata, duration: number): Promise<SyncLiteSubscriptionResponse> {
    return this.#unary(duration, (req, md, opts, cb) => this.#client.syncLiteSubscription(req, md, opts, cb),
      request, metadata);
  }
}
