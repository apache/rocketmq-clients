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

/**
 * Regression tests for the metrics export pipeline, aligned with the Java
 * client's org.apache.rocketmq.client.java.metrics package:
 *  - Metric switch parsing (on flag + endpoints)
 *  - Histogram bucket definitions / OTel view construction
 *  - ClientMeter / ClientMeterManager enable-disable lifecycle
 *  - MessageMeterInterceptor recording at the SEND/RECEIVE/CONSUME hook points
 *  - PushConsumerGaugeObserver aggregation of cached message count/bytes
 */

import { describe, it } from 'node:test';
import * as assert from 'node:assert';
import { Metadata } from '@grpc/grpc-js';
import { Attributes } from '@opentelemetry/api';
import { InstrumentType } from '@opentelemetry/sdk-metrics';
import {
  Endpoints as EndpointsPB,
  AddressScheme,
  Metric as MetricPB,
} from '../proto/apache/rocketmq/v2/definition_pb';
import { Producer } from '../src/producer';
import { ProcessQueue } from '../src/consumer/ProcessQueue';
import { PushConsumerGaugeObserver } from '../src/consumer/PushConsumerGaugeObserver';
import { Metric } from '../src/metrics/Metric';
import { ClientMeter } from '../src/metrics/ClientMeter';
import { ClientMeterManager } from '../src/metrics/ClientMeterManager';
import { EmptyGaugeObserver } from '../src/metrics/EmptyGaugeObserver';
import { GaugeEnum } from '../src/metrics/GaugeEnum';
import {
  HistogramEnum,
  HISTOGRAM_BUCKETS,
  buildHistogramView,
} from '../src/metrics/HistogramEnum';
import { InvocationStatus } from '../src/metrics/InvocationStatus';
import { MetricLabels } from '../src/metrics/MetricLabels';
import {
  MessageHookPoints,
  MessageHookPointsStatus,
  MessageInterceptorContextImpl,
  MessageMeterInterceptor,
} from '../src/hook';

const silentLogger = {
  info() { /* noop */ },
  warn() { /* noop */ },
  error() { /* noop */ },
  debug() { /* noop */ },
};

function buildMetricPb(on: boolean, host = '127.0.0.1', port = 10811): MetricPB {
  const endpointsPb = new EndpointsPB();
  endpointsPb.setScheme(AddressScheme.IPV4);
  endpointsPb.addAddresses().setHost(host).setPort(port);
  return new MetricPB().setOn(on).setEndpoints(endpointsPb);
}

/**
 * Captures every histogram record instead of exporting it, so the interceptor
 * can be exercised without standing up the OTLP pipeline.
 */
class RecordingMeterManager {
  readonly records: Array<{ histogramEnum: HistogramEnum; attributes: Attributes; value: number }> = [];
  enabled = true;

  isEnabled(): boolean {
    return this.enabled;
  }

  record(histogramEnum: HistogramEnum, attributes: Attributes, value: number): void {
    this.records.push({ histogramEnum, attributes, value });
  }

  setGaugeObserver(): void {
    // not exercised by the interceptor
  }

  async shutdown(): Promise<void> {
    // noop
  }
}

function asManager(recorder: RecordingMeterManager): ClientMeterManager {
  return recorder as unknown as ClientMeterManager;
}

describe('Metric switch parsing (vs Java metrics.Metric)', () => {
  it('should be enabled when on=true and endpoints are present', () => {
    const metric = new Metric(buildMetricPb(true, '127.0.0.1', 10811));
    assert.strictEqual(metric.on, true);
    assert.ok(metric.endpoints, 'expected endpoints to be parsed');
    assert.deepStrictEqual(metric.endpoints!.addressesList, [{ host: '127.0.0.1', port: 10811 }]);
  });

  it('should be disabled when the on flag is false', () => {
    const metric = new Metric(buildMetricPb(false));
    assert.strictEqual(metric.on, false);
    assert.strictEqual(metric.endpoints, null);
  });

  it('should be disabled when the metric is absent', () => {
    const metric = new Metric(undefined);
    assert.strictEqual(metric.on, false);
    assert.strictEqual(metric.endpoints, null);
  });
});

describe('Histogram buckets and views', () => {
  it('should define explicit buckets for all four histograms', () => {
    const histograms = Object.values(HistogramEnum);
    assert.strictEqual(histograms.length, 4);
    for (const histogram of histograms) {
      assert.ok(HISTOGRAM_BUCKETS[histogram].length > 0,
        `expected buckets for ${histogram}`);
    }
  });

  it('should build a view targeting the histogram instrument', () => {
    const view = buildHistogramView(HistogramEnum.SEND_COST_TIME);
    assert.strictEqual(view.instrumentName, HistogramEnum.SEND_COST_TIME);
    assert.strictEqual(view.instrumentType, InstrumentType.HISTOGRAM);
    assert.ok(view.aggregation, 'expected an explicit bucket aggregation');
  });
});

describe('ClientMeter lifecycle', () => {
  it('should be a no-op when disabled', () => {
    const meter = ClientMeter.disabledInstance();
    assert.strictEqual(meter.enabled, false);
    // must not throw even though no underlying meter exists
    meter.record(HistogramEnum.SEND_COST_TIME, {}, 1);
  });

  it('should only be satisfied by a matching switch', () => {
    const meter = ClientMeter.disabledInstance();
    assert.strictEqual(meter.satisfy(new Metric(undefined)), true);
    assert.strictEqual(meter.satisfy(new Metric(buildMetricPb(true))), false);
  });
});

describe('ClientMeterManager reset', () => {
  it('should keep metrics disabled while the switch is off', async () => {
    const manager = new ClientMeterManager('client-1', () => new Metadata(), silentLogger);
    assert.strictEqual(manager.isEnabled(), false);
    await manager.reset(new Metric(buildMetricPb(false)));
    assert.strictEqual(manager.isEnabled(), false);
    // idempotent: a second reset must not throw
    await manager.reset(new Metric(undefined));
    assert.strictEqual(manager.isEnabled(), false);
    await manager.shutdown();
  });

  it('should build the OTLP pipeline when the switch is on', async () => {
    const manager = new ClientMeterManager('client-2', () => new Metadata(), silentLogger);
    try {
      // 127.0.0.1:65535 is a closed port: the pipeline is built eagerly, while
      // the periodic export (60s) never fires during the test.
      await manager.reset(new Metric(buildMetricPb(true, '127.0.0.1', 65535)));
      assert.strictEqual(manager.isEnabled(), true, 'expected the meter to be enabled');
    } finally {
      await manager.shutdown();
    }
  });
});

describe('MessageMeterInterceptor (SEND/RECEIVE/CONSUME)', () => {
  it('should record send cost time on the SEND hook point', () => {
    const recorder = new RecordingMeterManager();
    const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => undefined);
    const context = new MessageInterceptorContextImpl(MessageHookPoints.SEND);
    const message = { topic: 'TopicSend' } as any;
    interceptor.doBefore(context, [ message ]);
    interceptor.doAfter(
      MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), [ message ]);

    assert.strictEqual(recorder.records.length, 1);
    const record = recorder.records[0];
    assert.strictEqual(record.histogramEnum, HistogramEnum.SEND_COST_TIME);
    assert.strictEqual(record.attributes[MetricLabels.TOPIC], 'TopicSend');
    assert.strictEqual(record.attributes[MetricLabels.CLIENT_ID], 'client-1');
    assert.strictEqual(record.attributes[MetricLabels.INVOCATION_STATUS], InvocationStatus.SUCCESS.name);
    assert.ok(record.value >= 0, 'expected a non-negative cost');
  });

  it('should mark the send as failed when the hook status is ERROR', () => {
    const recorder = new RecordingMeterManager();
    const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => undefined);
    const context = new MessageInterceptorContextImpl(MessageHookPoints.SEND);
    const message = { topic: 'TopicSend' } as any;
    interceptor.doBefore(context, [ message ]);
    interceptor.doAfter(
      MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.ERROR), [ message ]);

    assert.strictEqual(recorder.records.length, 1);
    assert.strictEqual(recorder.records[0].attributes[MetricLabels.INVOCATION_STATUS],
      InvocationStatus.FAILURE.name);
  });

  it('should record delivery latency on the RECEIVE hook point', () => {
    const recorder = new RecordingMeterManager();
    const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => 'group-1');
    const context = new MessageInterceptorContextImpl(MessageHookPoints.RECEIVE);
    const message = { topic: 'TopicRecv', transportDeliveryTimestamp: new Date(Date.now() - 120) } as any;
    interceptor.doBefore(context, [ message ]);
    interceptor.doAfter(
      MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), [ message ]);

    assert.strictEqual(recorder.records.length, 1);
    const record = recorder.records[0];
    assert.strictEqual(record.histogramEnum, HistogramEnum.DELIVERY_LATENCY);
    assert.strictEqual(record.attributes[MetricLabels.TOPIC], 'TopicRecv');
    assert.strictEqual(record.attributes[MetricLabels.CONSUMER_GROUP], 'group-1');
    // 120ms elapsed between the delivery timestamp and now
    assert.ok(record.value >= 100 && record.value < 5000, `unexpected latency ${record.value}`);
  });

  it('should skip delivery latency when the timestamp is in the future', () => {
    const recorder = new RecordingMeterManager();
    const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => 'group-1');
    const context = new MessageInterceptorContextImpl(MessageHookPoints.RECEIVE);
    const message = { topic: 'TopicRecv', transportDeliveryTimestamp: new Date(Date.now() + 60_000) } as any;
    interceptor.doBefore(context, [ message ]);
    interceptor.doAfter(
      MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), [ message ]);

    assert.strictEqual(recorder.records.length, 0, 'negative latency must not be recorded');
  });

  it('should record await time and process time on the CONSUME hook point', () => {
    const recorder = new RecordingMeterManager();
    const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => 'group-1');
    const context = new MessageInterceptorContextImpl(MessageHookPoints.CONSUME);
    const message = { topic: 'TopicConsume', decodeTimestamp: new Date(Date.now() - 200) } as any;
    interceptor.doBefore(context, [ message ]);
    interceptor.doAfter(
      MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), [ message ]);

    assert.strictEqual(recorder.records.length, 2);
    assert.strictEqual(recorder.records[0].histogramEnum, HistogramEnum.AWAIT_TIME);
    assert.ok(recorder.records[0].value >= 200, `unexpected await time ${recorder.records[0].value}`);
    assert.strictEqual(recorder.records[1].histogramEnum, HistogramEnum.PROCESS_TIME);
    assert.strictEqual(recorder.records[1].attributes[MetricLabels.CONSUMER_GROUP], 'group-1');
    assert.strictEqual(recorder.records[1].attributes[MetricLabels.INVOCATION_STATUS],
      InvocationStatus.SUCCESS.name);
  });

  it('should record nothing when metrics are disabled', () => {
    const recorder = new RecordingMeterManager();
    recorder.enabled = false;
    const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => 'group-1');
    const context = new MessageInterceptorContextImpl(MessageHookPoints.SEND);
    const message = { topic: 'TopicSend' } as any;
    interceptor.doBefore(context, [ message ]);
    interceptor.doAfter(
      MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), [ message ]);
    assert.strictEqual(recorder.records.length, 0);
  });

  it('should record nothing on CONSUME when the client has no consumer group', () => {
    const recorder = new RecordingMeterManager();
    const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => undefined);
    const context = new MessageInterceptorContextImpl(MessageHookPoints.CONSUME);
    const message = { topic: 'TopicConsume', decodeTimestamp: new Date(Date.now() - 50) } as any;
    interceptor.doBefore(context, [ message ]);
    interceptor.doAfter(
      MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), [ message ]);
    assert.strictEqual(recorder.records.length, 0);
  });
});

describe('Consumer gauge observers', () => {
  function fakeProcessQueue(topic: string, count: number, bytes: number): ProcessQueue {
    return {
      topic,
      cachedMessagesCount: () => count,
      cachedMessageBytes: () => bytes,
    } as unknown as ProcessQueue;
  }

  function toEntries(values: Map<Attributes, number>) {
    return Array.from(values.entries())
      .map(([ attributes, value ]) => ({
        topic: attributes[MetricLabels.TOPIC],
        group: attributes[MetricLabels.CONSUMER_GROUP],
        clientId: attributes[MetricLabels.CLIENT_ID],
        value,
      }))
      .sort((a, b) => String(a.topic).localeCompare(String(b.topic)));
  }

  it('should aggregate cached messages and bytes per topic', () => {
    const observer = new PushConsumerGaugeObserver(
      () => [
        fakeProcessQueue('TopicA', 3, 300),
        fakeProcessQueue('TopicA', 2, 200),
        fakeProcessQueue('TopicB', 5, 500),
      ],
      'client-1', 'group-1');

    assert.deepStrictEqual(observer.getGauges(),
      [ GaugeEnum.CONSUMER_CACHED_MESSAGES, GaugeEnum.CONSUMER_CACHED_BYTES ]);

    assert.deepStrictEqual(toEntries(observer.getValues(GaugeEnum.CONSUMER_CACHED_MESSAGES)), [
      { topic: 'TopicA', group: 'group-1', clientId: 'client-1', value: 5 },
      { topic: 'TopicB', group: 'group-1', clientId: 'client-1', value: 5 },
    ]);
    assert.deepStrictEqual(toEntries(observer.getValues(GaugeEnum.CONSUMER_CACHED_BYTES)), [
      { topic: 'TopicA', group: 'group-1', clientId: 'client-1', value: 500 },
      { topic: 'TopicB', group: 'group-1', clientId: 'client-1', value: 500 },
    ]);
  });

  it('should return an empty map when no process queue is assigned', () => {
    const observer = new PushConsumerGaugeObserver(() => [], 'client-1', 'group-1');
    assert.strictEqual(observer.getValues(GaugeEnum.CONSUMER_CACHED_MESSAGES).size, 0);
    assert.strictEqual(observer.getValues(GaugeEnum.CONSUMER_CACHED_BYTES).size, 0);
  });

  it('should expose no gauges through the empty observer', () => {
    assert.deepStrictEqual(EmptyGaugeObserver.EMPTY.getGauges(), []);
    assert.strictEqual(EmptyGaugeObserver.EMPTY.getValues().size, 0);
  });
});

describe('BaseClient metrics wiring', () => {
  it('should create the meter manager during construction', () => {
    const producer = new Producer({
      endpoints: '127.0.0.1:8081',
      namespace: '',
      topics: [ 'TopicTest' ],
    });
    const manager = (producer as any).clientMeterManager;
    assert.ok(manager instanceof ClientMeterManager, 'expected a ClientMeterManager to be created');
    assert.strictEqual(manager.isEnabled(), false, 'metrics start disabled until the settings command');
  });
});
