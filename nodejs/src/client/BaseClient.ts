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

import { debuglog } from 'node:util';
import { randomUUID } from 'node:crypto';
import { Metadata } from '@grpc/grpc-js';
import {
  Settings as SettingsPB,
  Status,
  ClientType,
  Code,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  QueryRouteRequest,
  RecoverOrphanedTransactionCommand,
  VerifyMessageCommand,
  PrintThreadStackTraceCommand,
  TelemetryCommand,
  ThreadStackTrace,
  HeartbeatRequest,
  NotifyClientTerminationRequest,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { createResource } from '../util';
import { TopicRouteData, Endpoints } from '../route';
import { StatusChecker } from '../exception';
import { Settings } from './Settings';
import { UserAgent } from './UserAgent';
import { Logger } from './Logger';
import { SessionCredentials } from './SessionCredentials';
import { RpcClientManager } from './RpcClientManager';
import { TelemetrySession } from './TelemetrySession';
import { ClientId } from './ClientId';

const debug = debuglog('rocketmq-client-nodejs:client:BaseClient');

export interface BaseClientOptions {
  sslEnabled?: boolean;
  /**
   * rocketmq cluster endpoints, e.g.:
   * - 127.0.0.1:8081;127.0.0.1:8082
   * - 127.0.0.1:8081
   * - example.com
   * - example.com:8443
   */
  endpoints: string;
  sessionCredentials?: SessionCredentials;
  requestTimeout?: number;
  logger?: Logger;
  topics?: string[];
}

/**
 * RocketMQ Base Client, Consumer and Producer should extends this class
 *
 * it handle:
 *  - RpcClient lifecycle, e.g: cleanup the idle clients
 *  - startup flow
 *  - periodic Task
 */
export abstract class BaseClient {
  readonly clientId = ClientId.create();
  readonly clientType = ClientType.CLIENT_TYPE_UNSPECIFIED;
  readonly sslEnabled: boolean;
  protected readonly sessionCredentials?: SessionCredentials;
  protected readonly endpoints: Endpoints;
  protected readonly isolated = new Map<string, Endpoints>();
  protected readonly requestTimeout: number;
  protected readonly topics = new Set<string>();
  protected readonly topicRouteCache = new Map<string, TopicRouteData>();
  protected readonly logger: Logger;
  protected readonly rpcClientManager: RpcClientManager;
  readonly #telemetrySessions = new Map<string, TelemetrySession>();
  #startupResolve?: () => void;
  // #startupReject?: (err: Error) => void;

  constructor(options: BaseClientOptions) {
    this.logger = options.logger || console;
    this.sslEnabled = options.sslEnabled === true;
    this.endpoints = new Endpoints(options.endpoints);
    this.sessionCredentials = options.sessionCredentials;
    this.requestTimeout = options.requestTimeout || 3000;
    this.rpcClientManager = new RpcClientManager(this, this.logger);
    if (options.topics) {
      for (const topic of options.topics) {
        this.topics.add(topic);
      }
    }
  }

  /**
   * Startup flow
   * https://github.com/apache/rocketmq-clients/blob/master/docs/workflow.md#startup
   */
  async startup() {
    this.logger.info('[Client=%s] Begin to startup the rocketmq client', this.clientId);
    try {
      await this.#startup();
    } catch (e) {
      const err = new Error(`[Client=${this.clientId}] Startup the rocketmq client failed, error=${e}`);
      this.logger.error(err.message);
      err.cause = e;
      throw err;
    }
    this.logger.info('[Client=%s] Startup the rocketmq client successfully', this.clientId);
  }

  async #startup() {
    // fetch topic route
    await this.updateRoutes();
    // update topic route every 30s
    setInterval(async () => {
      this.updateRoutes();
    }, 30000);

    // sync settings every 5m
    setInterval(async () => {
      this.#syncSettings();
    }, 5 * 60000);

    // heartbeat every 10s
    setInterval(async () => {
      this.#doHeartbeat();
    }, 5 * 60000);

    // doStats every 60s
    // doStats()

    if (this.topics.size > 0) {
      // wait for this first onSettingsCommand call
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
      await new Promise<void>((resolve, _reject) => {
        // this.#startupReject = reject;
        this.#startupResolve = resolve;
      });
      // this.#startupReject = undefined;
      this.#startupResolve = undefined;
    }
  }

  async shutdown() {
    this.logger.info('[Client=%s] Begin to shutdown the rocketmq client', this.clientId);
    await this.#notifyClientTermination();
    // telemetryCommandExecutor.shutdown();
    // if (!ExecutorServices.awaitTerminated(telemetryCommandExecutor)) {
    //   log.error('[Bug] Timeout to shutdown the telemetry command executor, clientId={}', clientId);
    // } else {
    //   log.info('Shutdown the telemetry command executor successfully, clientId={}', clientId);
    // }
    // log.info('Begin to release all telemetry sessions, clientId={}', clientId);
    // releaseClientSessions();
    // log.info('Release all telemetry sessions successfully, clientId={}', clientId);
    // clientManager.stopAsync().awaitTerminated();
    // clientCallbackExecutor.shutdown();
    // if (!ExecutorServices.awaitTerminated(clientCallbackExecutor)) {
    //   log.error('[Bug] Timeout to shutdown the client callback executor, clientId={}', clientId);
    // }
    // clientMeterManager.shutdown();
    this.logger.info('[Client=%s] Shutdown the rocketmq client successfully', this.clientId);
  }

  async #doHeartbeat() {
    const request = this.wrapHeartbeatRequest();
    for (const endpoints of this.getTotalRouteEndpoints()) {
      await this.rpcClientManager.heartbeat(endpoints, request, this.requestTimeout);
    }
  }

  #getTotalRouteEndpointsMap() {
    const endpointsMap = new Map<string, Endpoints>();
    for (const topicRoute of this.topicRouteCache.values()) {
      for (const endpoints of topicRoute.getTotalEndpoints()) {
        endpointsMap.set(endpoints.facade, endpoints);
      }
    }
    return endpointsMap;
  }

  protected getTotalRouteEndpoints() {
    const endpointsMap = this.#getTotalRouteEndpointsMap();
    return Array.from(endpointsMap.values());
  }

  protected findNewRouteEndpoints(endpointsList: Endpoints[]) {
    const endpointsMap = this.#getTotalRouteEndpointsMap();
    const newEndpoints: Endpoints[] = [];
    for (const endpoints of endpointsList) {
      if (!endpointsMap.has(endpoints.facade)) {
        newEndpoints.push(endpoints);
      }
    }
    return newEndpoints;
  }

  protected async updateRoutes() {
    for (const topic of this.topics) {
      await this.#fetchTopicRoute(topic);
    }
  }

  protected async getRouteData(topic: string) {
    let topicRouteData = this.topicRouteCache.get(topic);
    if (!topicRouteData) {
      this.topics.add(topic);
      topicRouteData = await this.#fetchTopicRoute(topic);
    }
    return topicRouteData;
  }

  async #fetchTopicRoute(topic: string) {
    const req = new QueryRouteRequest();
    req.setTopic(createResource(topic));
    req.setEndpoints(this.endpoints.toProtobuf());
    const response = await this.rpcClientManager.queryRoute(this.endpoints, req, this.requestTimeout);
    StatusChecker.check(response.getStatus()?.toObject());
    const topicRouteData = new TopicRouteData(response.getMessageQueuesList());
    const newEndpoints = this.findNewRouteEndpoints(topicRouteData.getTotalEndpoints());
    for (const endpoints of newEndpoints) {
      // sync current settings to new endpoints
      this.getTelemetrySession(endpoints).syncSettings();
    }
    this.topicRouteCache.set(topic, topicRouteData);
    this.onTopicRouteDataUpdate(topic, topicRouteData);
    debug('fetchTopicRoute topic=%o topicRouteData=%j', topic, topicRouteData);
    return topicRouteData;
  }

  #syncSettings() {
    const command = this.settingsCommand();
    for (const endpoints of this.getTotalRouteEndpoints()) {
      this.telemetry(endpoints, command);
    }
  }

  settingsCommand() {
    const command = new TelemetryCommand();
    command.setSettings(this.getSettings().toProtobuf());
    return command;
  }

  getTelemetrySession(endpoints: Endpoints) {
    let session = this.#telemetrySessions.get(endpoints.facade);
    if (!session) {
      session = new TelemetrySession(this, endpoints, this.logger);
      this.#telemetrySessions.set(endpoints.facade, session);
    }
    return session;
  }

  createTelemetryStream(endpoints: Endpoints) {
    const metadata = this.getRequestMetadata();
    return this.rpcClientManager.telemetry(endpoints, metadata);
  }

  telemetry(endpoints: Endpoints, command: TelemetryCommand) {
    this.getTelemetrySession(endpoints).write(command);
  }

  getRequestMetadata() {
    // https://github.com/apache/rocketmq-clients/blob/master/docs/transport.md
    // Transport Header
    const metadata = new Metadata();
    // version of protocol
    metadata.set('x-mq-protocol', 'v2');
    // client unique identifier: mbp@78774@2@3549a8wsr
    metadata.set('x-mq-client-id', this.clientId);
    // current timestamp: 20210309T195445Z
    // metadata.set('x-mq-date-time', '20230905T021525Z');
    // request id for each gRPC header: f122a1e0-dbcf-4ca4-9db7-221903354be7
    metadata.set('x-mq-request-id', randomUUID());
    // language of client
    // FIXME: java.lang.IllegalArgumentException: No enum constant org.apache.rocketmq.remoting.protocol.LanguageCode.nodejs
    // https://github.com/apache/rocketmq/blob/master/remoting/src/main/java/org/apache/rocketmq/remoting/protocol/LanguageCode.java
    metadata.set('x-mq-language', 'HTTP');
    // version of client
    metadata.set('x-mq-client-version', UserAgent.INSTANCE.version);
    return metadata;
  }

  protected abstract getSettings(): Settings;

  /**
   * Wrap heartbeat request
   */
  protected abstract wrapHeartbeatRequest(): HeartbeatRequest;

  /**
   * Wrap notify client termination request.
   */
  protected abstract wrapNotifyClientTerminationRequest(): NotifyClientTerminationRequest;

  /**
   * Notify remote that current client is prepared to be terminated.
   */
  async #notifyClientTermination() {
    this.logger.info('[Client=%s] Notify remote that client is terminated', this.clientId);
    const request = this.wrapNotifyClientTerminationRequest();
    for (const endpoints of this.getTotalRouteEndpoints()) {
      await this.rpcClientManager.notifyClientTermination(endpoints, request, this.requestTimeout);
    }
  }

  // eslint-disable-next-line @typescript-eslint/no-unused-vars
  protected onTopicRouteDataUpdate(_topic: string, _topicRouteData: TopicRouteData) {
    // sub class can monitor topic route data change here
  }

  onSettingsCommand(_endpoints: Endpoints, settings: SettingsPB) {
    // final Metric metric = new Metric(settings.getMetric());
    // clientMeterManager.reset(metric);
    this.getSettings().sync(settings);
    this.logger.info('[Client=%s] Sync settings=%j', this.clientId, this.getSettings());
    this.#startupResolve && this.#startupResolve();
  }

  onRecoverOrphanedTransactionCommand(_endpoints: Endpoints, command: RecoverOrphanedTransactionCommand) {
    this.logger.warn('[BaseClient] Ignore orphaned transaction recovery command from remote, which is not expected, clientId=%s, command=%j',
      this.clientId, command.toObject());
    // const telemetryCommand = new TelemetryCommand();
    // telemetryCommand.setStatus(new Status().setCode(Code.NOT_IMPLEMENTED));
    // telemetryCommand.setRecoverOrphanedTransactionCommand(new RecoverOrphanedTransactionCommand());
    // this.telemetry(endpoints, telemetryCommand);
  }

  onVerifyMessageCommand(endpoints: Endpoints, command: VerifyMessageCommand) {
    const obj = command.toObject();
    this.logger.warn('[BaseClient] Ignore verify message command from remote, which is not expected, clientId=%s, command=%j',
      this.clientId, obj);
    const telemetryCommand = new TelemetryCommand();
    telemetryCommand.setStatus(new Status().setCode(Code.NOT_IMPLEMENTED));
    telemetryCommand.setVerifyMessageCommand(new VerifyMessageCommand().setNonce(obj.nonce));
    this.telemetry(endpoints, telemetryCommand);
  }

  onPrintThreadStackTraceCommand(endpoints: Endpoints, command: PrintThreadStackTraceCommand) {
    const obj = command.toObject();
    this.logger.warn('[BaseClient] Ignore orphaned transaction recovery command from remote, which is not expected, clientId=%s, command=%j',
      this.clientId, obj);
    const nonce = obj.nonce;
    const telemetryCommand = new TelemetryCommand();
    telemetryCommand.setThreadStackTrace(new ThreadStackTrace().setThreadStackTrace('mock stack').setNonce(nonce));
    telemetryCommand.setStatus(new Status().setCode(Code.OK));
    this.telemetry(endpoints, telemetryCommand);
  }
}
