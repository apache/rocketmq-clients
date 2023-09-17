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
  Settings as SettingsPB,
  ClientType,
  Subscription,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import { Endpoints } from '../route';
import { Settings, UserAgent } from '../client';
import { createDuration, createResource } from '../util';
import { FilterExpression } from './FilterExpression';

export class SimpleSubscriptionSettings extends Settings {
  readonly longPollingTimeout: number;
  readonly group: string;
  readonly subscriptionExpressions: Map<string, FilterExpression>;

  constructor(clientId: string, accessPoint: Endpoints, group: string,
    requestTimeout: number, longPollingTimeout: number, subscriptionExpressions: Map<string, FilterExpression>) {
    super(clientId, ClientType.SIMPLE_CONSUMER, accessPoint, requestTimeout);
    this.longPollingTimeout = longPollingTimeout;
    this.group = group;
    this.subscriptionExpressions = subscriptionExpressions;
  }

  toProtobuf(): SettingsPB {
    const subscription = new Subscription()
      .setGroup(createResource(this.group))
      .setLongPollingTimeout(createDuration(this.longPollingTimeout));

    for (const [ topic, filterExpression ] of this.subscriptionExpressions.entries()) {
      subscription.addSubscriptions()
        .setTopic(createResource(topic))
        .setExpression(filterExpression.toProtobuf());
    }
    return new SettingsPB()
      .setClientType(this.clientType)
      .setAccessPoint(this.accessPoint.toProtobuf())
      .setRequestTimeout(createDuration(this.requestTimeout))
      .setSubscription(subscription)
      .setUserAgent(UserAgent.INSTANCE.toProtobuf());
  }

  sync(settings: SettingsPB): void {
    if (settings.getPubSubCase() !== SettingsPB.PubSubCase.SUBSCRIPTION) {
      // log.error("[Bug] Issued settings not match with the client type, clientId={}, pubSubCase={}, "
      //       + "clientType={}", clientId, pubSubCase, clientType);
    }
  }
}
