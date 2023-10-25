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
  Publishing,
} from '../../proto/apache/rocketmq/v2/definition_pb';
import { Endpoints } from '../route';
import { ExponentialBackoffRetryPolicy } from '../retry';
import { Settings, UserAgent } from '../client';
import { createDuration } from '../util';

export class PublishingSettings extends Settings {
  readonly #topics: Set<string>;
  /**
   * If message body size exceeds the threshold, it would be compressed for convenience of transport.
   * https://rocketmq.apache.org/docs/introduction/03limits/
   * Default max message size is 4 MB
   */
  #maxBodySizeBytes = 4 * 1024 * 1024;
  #validateMessageType = true;

  constructor(clientId: string, accessPoint: Endpoints, retryPolicy: ExponentialBackoffRetryPolicy,
    requestTimeout: number, topics: Set<string>) {
    super(clientId, ClientType.PRODUCER, accessPoint, requestTimeout, retryPolicy);
    this.#topics = topics;
  }

  get maxBodySizeBytes() {
    return this.#maxBodySizeBytes;
  }

  isValidateMessageType() {
    return this.#validateMessageType;
  }

  toProtobuf(): SettingsPB {
    const publishing = new Publishing()
      .setValidateMessageType(this.#validateMessageType);
    for (const topic of this.#topics) {
      publishing.addTopics().setName(topic);
    }
    return new SettingsPB()
      .setClientType(this.clientType)
      .setAccessPoint(this.accessPoint.toProtobuf())
      .setRequestTimeout(createDuration(this.requestTimeout))
      .setPublishing(publishing)
      .setUserAgent(UserAgent.INSTANCE.toProtobuf());
  }

  sync(settings: SettingsPB): void {
    if (settings.getPubSubCase() !== SettingsPB.PubSubCase.PUBLISHING) {
      // log.error("[Bug] Issued settings not match with the client type, clientId={}, pubSubCase={}, "
      //       + "clientType={}", clientId, pubSubCase, clientType);
      return;
    }
    const backoffPolicy = settings.getBackoffPolicy()!;
    const publishing = settings.getPublishing()!.toObject();
    const exist = this.retryPolicy!;
    this.retryPolicy = exist.inheritBackoff(backoffPolicy);
    this.#validateMessageType = publishing.validateMessageType;
    this.#maxBodySizeBytes = publishing.maxBodySize;
  }
}
