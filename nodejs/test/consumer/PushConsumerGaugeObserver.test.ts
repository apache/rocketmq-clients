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

import * as assert from 'node:assert';
import { Assignment } from '../../src/consumer/Assignment';
import { Assignments } from '../../src/consumer/Assignments';
import type { ProcessQueue } from '../../src/consumer/ProcessQueue';
import { PushConsumerGaugeObserver } from '../../src/consumer/PushConsumerGaugeObserver';
import { GaugeEnum } from '../../src/metrics/GaugeEnum';
import { MetricLabels } from '../../src/metrics/MetricLabels';
import { buildRoute } from '../support/protobuf';

function assignments(queueIds: number[]) {
  // A fresh route per call, so equality never relies on object identity.
  const queues = buildRoute({ queuesPerBroker: 4 }).messageQueues;
  return new Assignments(queueIds.map(id => new Assignment(queues[id])));
}

describe('Assignments', () => {
  it('compares assignments by message queue in order', () => {
    assert.ok(assignments([ 0, 1 ]).equals(assignments([ 0, 1 ])));
    assert.strictEqual(assignments([ 0, 1 ]).hashCode(), assignments([ 0, 1 ]).hashCode());
    assert.ok(!assignments([ 0, 1 ]).equals(assignments([ 1, 0 ])));
    assert.ok(!assignments([ 0, 1 ]).equals(assignments([ 0 ])));
    assert.ok(!assignments([ 0 ]).equals(undefined));
    assert.ok(assignments([]).equals(assignments([])));
  });

  it('distinguishes queues of different topics', () => {
    const other = new Assignments([ new Assignment(buildRoute({ topic: 'topic-b' }).messageQueues[0]) ]);
    assert.ok(!assignments([ 0 ]).equals(other));
  });
});

describe('PushConsumerGaugeObserver', () => {
  const queue = (topic: string, count: number, bytes: number) =>
    ({ topic, cachedMessagesCount: () => count, cachedMessageBytes: () => bytes }) as unknown as ProcessQueue;
  const observer = new PushConsumerGaugeObserver(
    () => [ queue('topic-a', 1, 10), queue('topic-a', 2, 20), queue('topic-b', 3, 30) ], 'client-a', 'group-a');

  function valuesByTopic(gauge: GaugeEnum) {
    const result = new Map<unknown, number>();
    for (const [ attributes, value ] of observer.getValues(gauge)) {
      assert.strictEqual(attributes[MetricLabels.CLIENT_ID], 'client-a');
      assert.strictEqual(attributes[MetricLabels.CONSUMER_GROUP], 'group-a');
      result.set(attributes[MetricLabels.TOPIC], value);
    }
    return result;
  }

  it('reports both consumer cache gauges', () => {
    assert.deepStrictEqual(observer.getGauges(), [ GaugeEnum.CONSUMER_CACHED_MESSAGES, GaugeEnum.CONSUMER_CACHED_BYTES ]);
  });

  it('sums the cached message count and bytes of all queues per topic', () => {
    assert.deepStrictEqual(valuesByTopic(GaugeEnum.CONSUMER_CACHED_MESSAGES), new Map([[ 'topic-a', 3 ], [ 'topic-b', 3 ]]));
    assert.deepStrictEqual(valuesByTopic(GaugeEnum.CONSUMER_CACHED_BYTES), new Map([[ 'topic-a', 30 ], [ 'topic-b', 30 ]]));
  });

  it('reports nothing without process queues', () => {
    const empty = new PushConsumerGaugeObserver(() => [], 'client-a', 'group-a');
    assert.strictEqual(empty.getValues(GaugeEnum.CONSUMER_CACHED_MESSAGES).size, 0);
  });
});
