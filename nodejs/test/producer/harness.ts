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

import { Code, MessageType } from '../../proto/apache/rocketmq/v2/definition_pb';
import {
  NotifyClientTerminationResponse,
  QueryRouteResponse,
  SendMessageResponse,
  SendResultEntry,
} from '../../proto/apache/rocketmq/v2/service_pb';
import { Producer, ProducerOptions } from '../../src/producer';
import { TopicRouteData } from '../../src/route';
import { createSilentLogger, FakeTelemetryStream, installFakeRpcClientManager } from '../support/fakes';
import { buildRoute, buildStatus } from '../support/protobuf';

export const TOPIC = 'test-topic';
export const BROKERS = [
  { name: 'broker-a', endpoints: '127.0.0.1:8081' },
  { name: 'broker-b', endpoints: '127.0.0.1:8082' },
  { name: 'broker-c', endpoints: '127.0.0.1:8083' },
];
const ALL_MESSAGE_TYPES = [
  MessageType.NORMAL, MessageType.FIFO, MessageType.DELAY, MessageType.TRANSACTION,
  MessageType.PRIORITY, MessageType.LITE,
];

export function routeResponse(route: TopicRouteData = buildRoute({ topic: TOPIC, brokers: BROKERS }),
  acceptMessageTypes = ALL_MESSAGE_TYPES) {
  return new QueryRouteResponse().setStatus(buildStatus())
    .setMessageQueuesList(route.messageQueues.map(mq => mq.toProtobuf().setAcceptMessageTypesList(acceptMessageTypes)));
}

export function sendResponse(entries: Array<{ messageId?: string; transactionId?: string; code?: Code }> = [{}],
  code = Code.OK) {
  return new SendMessageResponse().setStatus(buildStatus(code, 'remote status'))
    .setEntriesList(entries.map((entry, i) => new SendResultEntry()
      .setStatus(buildStatus(entry.code ?? code, 'remote status'))
      .setMessageId(entry.messageId ?? `message-id-${i}`)
      .setTransactionId(entry.transactionId ?? `transaction-id-${i}`)
      .setRecallHandle(`recall-handle-${i}`)
      .setOffset(i)));
}

// Without configured topics, startup() does not wait for server settings; routes are queried on the first send.
export async function startProducer(options: Partial<ProducerOptions> = {}) {
  const logger = createSilentLogger();
  const producer = new Producer({ endpoints: '127.0.0.1:8080', namespace: '', logger, ...options });
  const rpc = installFakeRpcClientManager(producer);
  rpc.queryRoute.resolves(routeResponse());
  rpc.telemetry.callsFake(() => new FakeTelemetryStream().asGrpcStream());
  rpc.notifyClientTermination.resolves(new NotifyClientTerminationResponse().setStatus(buildStatus()));
  await producer.startup();
  return { producer, rpc, logger };
}
