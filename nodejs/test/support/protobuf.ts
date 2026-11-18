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

import { gzipSync } from 'node:zlib';
import {
  Broker, ClientType, Code, Digest, DigestType, Encoding, ExponentialBackoff,
  Message, MessageQueue, MessageType, Permission, Publishing, RetryPolicy,
  Settings, Status, Subscription, SystemProperties,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import { Endpoints, TopicRouteData } from '../../src/route';
import { createDuration, createResource, crc32CheckSum, md5CheckSum, sha1CheckSum } from '../../src/util';

export function buildStatus(code = Code.OK, message = ''): Status {
  return new Status().setCode(code).setMessage(message);
}

interface RouteOptions {
  topic?: string;
  namespace?: string;
  brokers?: Array<{ name: string; id?: number; endpoints: string }>;
  queuesPerBroker?: number;
  brokerId?: number;
  permission?: Permission;
}

export function buildRoute(options: RouteOptions = {}): TopicRouteData {
  const brokers = options.brokers ?? [{ name: 'broker-0', endpoints: '127.0.0.1:8081' }];
  const queues: MessageQueue[] = [];
  for (const broker of brokers) {
    const protoBroker = new Broker().setName(broker.name)
      .setId(broker.id ?? options.brokerId ?? 0)
      .setEndpoints(new Endpoints(broker.endpoints).toProtobuf());
    for (let id = 0; id < (options.queuesPerBroker ?? 1); id++) {
      queues.push(new MessageQueue().setId(id)
        .setTopic(createResource(options.topic ?? 'test-topic').setResourceNamespace(options.namespace ?? ''))
        .setBroker(protoBroker)
        .setPermission(options.permission ?? Permission.READ_WRITE));
    }
  }
  return new TopicRouteData(queues);
}

interface MessageOptions {
  topic?: string;
  body?: Buffer;
  gzip?: boolean;
  digestType?: DigestType;
  checksum?: string;
  messageId?: string;
  messageType?: MessageType;
  receiptHandle?: string;
  deliveryAttempt?: number;
  tag?: string;
  keys?: string[];
  messageGroup?: string;
  liteTopic?: string;
  priority?: number;
  properties?: Map<string, string>;
}

export function buildMessagePB(options: MessageOptions = {}): Message {
  const rawBody = options.body ?? Buffer.from('test-body');
  const body = options.gzip ? gzipSync(rawBody) : rawBody;
  const digestType = options.digestType ?? DigestType.CRC32;
  let checksum: string;
  switch (digestType) {
    case DigestType.CRC32: checksum = crc32CheckSum(body); break;
    case DigestType.MD5: checksum = md5CheckSum(body); break;
    case DigestType.SHA1: checksum = sha1CheckSum(body); break;
    default: throw new Error(`不支持的测试 digestType: ${digestType}`);
  }
  const properties = new SystemProperties()
    .setMessageId(options.messageId ?? 'test-message-id')
    .setMessageType(options.messageType ?? MessageType.NORMAL)
    .setReceiptHandle(options.receiptHandle ?? 'test-receipt-handle')
    .setDeliveryAttempt(options.deliveryAttempt ?? 1)
    .setBodyEncoding(options.gzip ? Encoding.GZIP : Encoding.IDENTITY)
    .setBodyDigest(new Digest().setType(digestType).setChecksum(options.checksum ?? checksum));
  if (options.tag !== undefined) properties.setTag(options.tag);
  if (options.keys !== undefined) properties.setKeysList(options.keys);
  if (options.messageGroup !== undefined) properties.setMessageGroup(options.messageGroup);
  if (options.liteTopic !== undefined) properties.setLiteTopic(options.liteTopic);
  if (options.priority !== undefined) properties.setPriority(options.priority);
  const message = new Message().setTopic(createResource(options.topic ?? 'test-topic'))
    .setBody(body)
    .setSystemProperties(properties);
  for (const [ key, value ] of options.properties ?? []) {
    message.getUserPropertiesMap().set(key, value);
  }
  return message;
}

export function buildSettings(options: {
  clientType?: ClientType;
  requestTimeout?: number;
  retryPolicy?: RetryPolicy;
  publishing?: Publishing;
  subscription?: Subscription;
} = {}): Settings {
  const retryPolicy = options.retryPolicy ?? new RetryPolicy().setMaxAttempts(3)
    .setExponentialBackoff(new ExponentialBackoff()
      .setInitial(createDuration(1000)).setMax(createDuration(10000))
      .setMultiplier(2));
  const settings = new Settings().setClientType(options.clientType ?? ClientType.PRODUCER)
    .setRequestTimeout(createDuration(options.requestTimeout ?? 3000))
    .setBackoffPolicy(retryPolicy);
  if (options.subscription) {
    settings.setSubscription(options.subscription);
  } else {
    settings.setPublishing(options.publishing ?? new Publishing()
      .setMaxBodySize(4 * 1024 * 1024).setValidateMessageType(true));
  }
  return settings;
}
