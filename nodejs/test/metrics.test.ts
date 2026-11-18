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

import * as assert from 'node:assert';
import * as sinon from 'sinon';
import { Metadata } from '@grpc/grpc-js';
import { Attributes, Meter } from '@opentelemetry/api';
import { ExplicitBucketHistogramAggregation, InstrumentType, MeterProvider as SdkMeterProvider } from '@opentelemetry/sdk-metrics';
import { Endpoints } from '../src/route';
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

  it('should be disabled when on=true but endpoints are absent', () => {
    const metric = new Metric(new MetricPB().setOn(true));
    assert.strictEqual(metric.on, false);
    assert.strictEqual(metric.endpoints, null);
    assert.strictEqual(new Metric(null).on, false);
  });
});

describe('Histogram buckets and views', () => {
  const javaHistograms: Array<[HistogramEnum, string, number[]]> = [
    [ HistogramEnum.SEND_COST_TIME, 'rocketmq_send_cost_time', [ 1, 5, 10, 20, 50, 200, 500 ]],
    [ HistogramEnum.DELIVERY_LATENCY, 'rocketmq_delivery_latency', [ 1, 5, 10, 20, 50, 200, 500 ]],
    [ HistogramEnum.AWAIT_TIME, 'rocketmq_await_time', [ 1, 5, 20, 100, 1000, 5000, 10000 ]],
    [ HistogramEnum.PROCESS_TIME, 'rocketmq_process_time', [ 1, 5, 10, 100, 1000, 10000, 60000 ]],
  ];
  for (const [ histogram, name, buckets ] of javaHistograms) {
    it(`should match the Java name, buckets and view for ${name}`, () => {
      assert.strictEqual(histogram, name);
      assert.deepStrictEqual(HISTOGRAM_BUCKETS[histogram], buckets);
      const view = buildHistogramView(histogram);
      assert.strictEqual(view.instrumentName, name);
      assert.strictEqual(view.instrumentType, InstrumentType.HISTOGRAM);
      assert.ok(view.aggregation instanceof ExplicitBucketHistogramAggregation);
    });
  }

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
  afterEach(() => sinon.restore());

  it('should be a no-op when disabled', async () => {
    const meter = ClientMeter.disabledInstance();
    assert.strictEqual(meter.enabled, false);
    // must not throw even though no underlying meter exists
    meter.record(HistogramEnum.SEND_COST_TIME, {}, 1);
    await meter.shutdown();
  });

  it('should only be satisfied by a matching switch', () => {
    const meter = ClientMeter.disabledInstance();
    assert.strictEqual(meter.satisfy(new Metric(undefined)), true);
    assert.strictEqual(meter.satisfy(new Metric(buildMetricPb(true))), false);
  });

  it('should cache histograms and preserve attributes and values', async () => {
    const record = sinon.spy();
    const createHistogram = sinon.stub().returns({ record });
    const meter = { createHistogram } as unknown as Meter;
    const provider = sinon.createStubInstance(SdkMeterProvider);
    provider.shutdown.resolves();
    const clientMeter = new ClientMeter(meter, new Endpoints('127.0.0.1:10811'), provider);
    const attributes = { topic: 'TopicA', client_id: 'client-1' };
    clientMeter.record(HistogramEnum.SEND_COST_TIME, attributes, 3);
    clientMeter.record(HistogramEnum.SEND_COST_TIME, attributes, 7);
    clientMeter.record(HistogramEnum.PROCESS_TIME, attributes, 11);
    assert.deepStrictEqual(createHistogram.args, [[ HistogramEnum.SEND_COST_TIME ], [ HistogramEnum.PROCESS_TIME ]]);
    assert.deepStrictEqual(record.args, [[ 3, attributes ], [ 7, attributes ], [ 11, attributes ]]);
    await clientMeter.shutdown();
    sinon.assert.calledOnce(provider.shutdown);
  });

  it('should only satisfy enabled settings with matching endpoints', () => {
    const meter = {} as Meter;
    const provider = sinon.createStubInstance(SdkMeterProvider);
    const clientMeter = new ClientMeter(meter, new Endpoints('127.0.0.1:10811'), provider);
    assert.strictEqual(clientMeter.satisfy(new Metric(buildMetricPb(true))), true);
    assert.strictEqual(clientMeter.satisfy(new Metric(buildMetricPb(false))), false);
    assert.strictEqual(clientMeter.satisfy(new Metric(buildMetricPb(true, '127.0.0.2'))), false);
    assert.strictEqual(clientMeter.satisfy(new Metric(buildMetricPb(true, '127.0.0.1', 10812))), false);
    assert.strictEqual(clientMeter.satisfy(new Metric(new MetricPB().setOn(true))), false);
  });

  it('should remain disabled when any required component is absent', async () => {
    const meter = { createHistogram: sinon.spy() } as unknown as Meter;
    const endpoints = new Endpoints('127.0.0.1:10811');
    const provider = sinon.createStubInstance(SdkMeterProvider);
    for (const clientMeter of [
      new ClientMeter(null, endpoints, provider),
      new ClientMeter(meter, null, provider),
      new ClientMeter(meter, endpoints, null),
    ]) {
      assert.strictEqual(clientMeter.enabled, false);
      clientMeter.record(HistogramEnum.SEND_COST_TIME, {}, 1);
      await clientMeter.shutdown();
    }
    sinon.assert.notCalled(provider.shutdown);
    sinon.assert.notCalled(meter.createHistogram as sinon.SinonSpy);
  });

  it('should not reject when provider shutdown fails', async () => {
    const provider = sinon.createStubInstance(SdkMeterProvider);
    provider.shutdown.rejects(new Error('shutdown failed'));
    const clientMeter = new ClientMeter({} as Meter, new Endpoints('127.0.0.1:10811'), provider);
    await clientMeter.shutdown();
    sinon.assert.calledOnce(provider.shutdown);
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

describe('MessageMeterInterceptor exact recording', () => {
  let clock: sinon.SinonFakeTimers;
  beforeEach(() => { clock = sinon.useFakeTimers({ now: 1000, toFake: [ 'Date' ] }); });
  afterEach(() => sinon.restore());

  ([
    [ MessageHookPointsStatus.OK, 'success' ],
    [ MessageHookPointsStatus.ERROR, 'failure' ],
  ] as const).forEach(([ status, label ]) => {
    it(`should record exact SEND elapsed time and ${label} labels`, () => {
      const recorder = new RecordingMeterManager();
      const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => undefined);
      const context = new MessageInterceptorContextImpl(MessageHookPoints.SEND);
      const messages = [{ topic: 'TopicA' }];
      interceptor.doBefore(context, messages);
      clock.tick(30);
      interceptor.doAfter(MessageInterceptorContextImpl.withStatus(context, status), messages);
      assert.deepStrictEqual(recorder.records, [{
        histogramEnum: HistogramEnum.SEND_COST_TIME,
        attributes: { topic: 'TopicA', client_id: 'client-1', invocation_status: label },
        value: 30,
      }]);
    });

    it(`should record exact CONSUME await time, process time and ${label} labels`, () => {
      const recorder = new RecordingMeterManager();
      const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => 'group-1');
      const context = new MessageInterceptorContextImpl(MessageHookPoints.CONSUME);
      const messages = [{ topic: 'TopicA', decodeTimestamp: new Date(900) }];
      interceptor.doBefore(context, messages);
      clock.tick(20);
      interceptor.doAfter(MessageInterceptorContextImpl.withStatus(context, status), messages);
      assert.deepStrictEqual(recorder.records, [
        { histogramEnum: HistogramEnum.AWAIT_TIME,
          attributes: { topic: 'TopicA', client_id: 'client-1', consumer_group: 'group-1' }, value: 100 },
        { histogramEnum: HistogramEnum.PROCESS_TIME,
          attributes: { topic: 'TopicA', client_id: 'client-1', consumer_group: 'group-1', invocation_status: label }, value: 20 },
      ]);
    });
  });

  it('should record only the first received message and allow zero delivery latency', () => {
    const recorder = new RecordingMeterManager();
    const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => 'group-1');
    const context = new MessageInterceptorContextImpl(MessageHookPoints.RECEIVE);
    interceptor.doAfter(context, [
      { topic: 'TopicA', transportDeliveryTimestamp: new Date(1000) },
      { topic: 'TopicB', transportDeliveryTimestamp: new Date(500) },
    ]);
    assert.deepStrictEqual(recorder.records, [{ histogramEnum: HistogramEnum.DELIVERY_LATENCY,
      attributes: { topic: 'TopicA', client_id: 'client-1', consumer_group: 'group-1' }, value: 0 }]);
  });

  it('should skip empty messages, missing timestamps, and unpaired after hooks', () => {
    const recorder = new RecordingMeterManager();
    const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => 'group-1');
    for (const point of Object.values(MessageHookPoints)) {
      const context = new MessageInterceptorContextImpl(point);
      interceptor.doAfter(context, [{ topic: 'TopicA' }]);
      interceptor.doBefore(context, []);
      interceptor.doAfter(context, []);
      interceptor.doBefore(context, [{ topic: 'TopicA' }]);
    }
    assert.deepStrictEqual(recorder.records, []);
  });

  it('should skip every hook when metrics are disabled', () => {
    const recorder = new RecordingMeterManager();
    recorder.enabled = false;
    const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => 'group-1');
    const messages = [{ topic: 'TopicA', decodeTimestamp: new Date(900), transportDeliveryTimestamp: new Date(900) }];
    for (const point of Object.values(MessageHookPoints)) {
      const context = new MessageInterceptorContextImpl(point);
      interceptor.doBefore(context, messages);
      interceptor.doAfter(MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), messages);
      assert.strictEqual(context.getAttributes().size, 0);
    }
    assert.deepStrictEqual(recorder.records, []);
  });

  it('should skip RECEIVE and CONSUME without a consumer group', () => {
    const recorder = new RecordingMeterManager();
    const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => undefined);
    const messages = [{ topic: 'TopicA', decodeTimestamp: new Date(900), transportDeliveryTimestamp: new Date(900) }];
    for (const point of [ MessageHookPoints.RECEIVE, MessageHookPoints.CONSUME ]) {
      const context = new MessageInterceptorContextImpl(point);
      interceptor.doBefore(context, messages);
      interceptor.doAfter(context, messages);
      assert.strictEqual(context.getAttributes().size, 0);
    }
    assert.deepStrictEqual(recorder.records, []);
  });

  it('should stop recording if metrics are disabled between before and after', () => {
    const recorder = new RecordingMeterManager();
    const interceptor = new MessageMeterInterceptor(asManager(recorder), 'client-1', () => 'group-1');
    const context = new MessageInterceptorContextImpl(MessageHookPoints.SEND);
    const messages = [{ topic: 'TopicA' }];
    interceptor.doBefore(context, messages);
    recorder.enabled = false;
    clock.tick(10);
    interceptor.doAfter(MessageInterceptorContextImpl.withStatus(context, MessageHookPointsStatus.OK), messages);
    assert.deepStrictEqual(recorder.records, []);
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
