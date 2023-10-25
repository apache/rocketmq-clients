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

import { Broker as BrokerPB } from '../../proto/apache/rocketmq/v2/definition_pb';
import { Endpoints } from './Endpoints';

export class Broker implements BrokerPB.AsObject {
  name: string;
  id: number;
  endpoints: Endpoints;

  constructor(broker: BrokerPB.AsObject) {
    this.name = broker.name;
    this.id = broker.id;
    this.endpoints = new Endpoints(broker.endpoints!);
  }

  toProtobuf() {
    const broker = new BrokerPB();
    broker.setName(this.name);
    broker.setId(this.id);
    broker.setEndpoints(this.endpoints.toProtobuf());
    return broker;
  }
}
