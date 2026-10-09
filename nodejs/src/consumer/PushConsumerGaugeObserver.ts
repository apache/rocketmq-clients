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

import { Attributes } from '@opentelemetry/api';
import { ProcessQueue } from './ProcessQueue';
import { GaugeEnum } from '../metrics/GaugeEnum';
import { GaugeObserver } from '../metrics/GaugeObserver';
import { MetricLabels } from '../metrics/MetricLabels';

/**
 * Aggregates the per-process-queue cache statistics of a push consumer into the
 * two RocketMQ consumer gauges, mirroring the GaugeObserver implementation held
 * by org.apache.rocketmq.client.java.impl.consumer.PushConsumerImpl.
 */
export class PushConsumerGaugeObserver implements GaugeObserver {
  constructor(
    private readonly processQueuesProvider: () => ProcessQueue[],
    private readonly clientId: string,
    private readonly consumerGroup: string,
  ) {
  }

  getGauges(): GaugeEnum[] {
    return [ GaugeEnum.CONSUMER_CACHED_MESSAGES, GaugeEnum.CONSUMER_CACHED_BYTES ];
  }

  getValues(gauge: GaugeEnum): Map<Attributes, number> {
    const result = new Map<Attributes, number>();
    const byTopic = new Map<string, number>();
    for (const pq of this.processQueuesProvider()) {
      const increment = gauge === GaugeEnum.CONSUMER_CACHED_MESSAGES
        ? pq.cachedMessagesCount()
        : pq.cachedMessageBytes();
      byTopic.set(pq.topic, (byTopic.get(pq.topic) ?? 0) + increment);
    }
    for (const [ topic, value ] of byTopic) {
      const attributes: Attributes = {
        [MetricLabels.TOPIC]: topic,
        [MetricLabels.CLIENT_ID]: this.clientId,
        [MetricLabels.CONSUMER_GROUP]: this.consumerGroup,
      };
      result.set(attributes, value);
    }
    return result;
  }
}
