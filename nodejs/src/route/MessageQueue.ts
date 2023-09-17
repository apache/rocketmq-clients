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

import {
  MessageQueue as MessageQueuePB, MessageType, Permission,
  Resource,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import { createResource } from '../util';
import { Broker } from './Broker';

export class MessageQueue {
  topic: Resource.AsObject;
  queueId: number;
  broker: Broker;
  permission: Permission;
  acceptMessageTypesList: MessageType[];

  constructor(messageQueue: MessageQueuePB) {
    this.topic = messageQueue.getTopic()!.toObject();
    this.queueId = messageQueue.getId();
    this.permission = messageQueue.getPermission();
    this.acceptMessageTypesList = messageQueue.getAcceptMessageTypesList();
    this.broker = new Broker(messageQueue.getBroker()!.toObject());
  }

  toProtobuf() {
    const messageQueue = new MessageQueuePB();
    messageQueue.setId(this.queueId);
    messageQueue.setTopic(createResource(this.topic.name));
    messageQueue.setBroker(this.broker.toProtobuf());
    messageQueue.setPermission(this.permission);
    messageQueue.setAcceptMessageTypesList(this.acceptMessageTypesList);
    return messageQueue;
  }
}
