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

import { strict as assert } from 'node:assert';
import * as sinon from 'sinon';
import { Metadata, Server, ServerCredentials, ServiceDefinition } from '@grpc/grpc-js';
import { Attributes } from '@opentelemetry/api';
import { MeterProvider as SdkMeterProvider } from '@opentelemetry/sdk-metrics';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-grpc';
import { AddressScheme, Endpoints as EndpointsPB, Metric as MetricPB } from '../../proto/apache/rocketmq/v2/definition_pb';
import { ClientMeter } from '../../src/metrics/ClientMeter';
import { ClientMeterManager } from '../../src/metrics/ClientMeterManager';
import { GaugeEnum } from '../../src/metrics/GaugeEnum';
import { GaugeObserver } from '../../src/metrics/GaugeObserver';
import { HistogramEnum } from '../../src/metrics/HistogramEnum';
import { Metric } from '../../src/metrics/Metric';
import { createSilentLogger, deferred } from '../support/fakes';

// 生命周期用例替换 exporter；地址仅用作配置哨兵，不连接网络。
const CLOSED_PORT = 65535;
const OTLP_METRICS_EXPORT_PATH = '/opentelemetry.proto.collector.metrics.v1.MetricsService/Export';

function metricOf(on: boolean, port = CLOSED_PORT, host = '127.0.0.1', scheme = AddressScheme.IPV4) {
  const endpoints = new EndpointsPB().setScheme(scheme);
  endpoints.addAddresses().setHost(host).setPort(port);
  return new Metric(new MetricPB().setOn(on).setEndpoints(endpoints));
}

function currentMeter(manager: ClientMeterManager): ClientMeter {
  return (manager as any).clientMeter;
}

/** 只记录 Export 调用 metadata 的 OTLP/gRPC collector。 */
async function startCollector(host = '127.0.0.1') {
  const received: Metadata[] = [];
  const passThrough = (value: Buffer) => value;
  const service = {
    Export: {
      path: OTLP_METRICS_EXPORT_PATH,
      requestStream: false,
      responseStream: false,
      requestSerialize: passThrough,
      requestDeserialize: passThrough,
      responseSerialize: passThrough,
      responseDeserialize: passThrough,
    },
  } as unknown as ServiceDefinition;
  const server = new Server();
  server.addService(service, {
    Export: (call: any, callback: any) => {
      received.push(call.metadata);
      // 空字节即合法的 ExportMetricsServiceResponse。
      callback(null, Buffer.alloc(0));
    },
  });
  const port = await new Promise<number>((resolve, reject) => {
    server.bindAsync(`${host.includes(':') ? `[${host}]` : host}:0`, ServerCredentials.createInsecure(), (err, boundPort) => {
      if (err) reject(err);
      else resolve(boundPort);
    });
  });
  return {
    port,
    received,
    close: () => new Promise<void>(resolve => server.tryShutdown(() => resolve())),
  };
}

describe('ClientMeterManager lifecycle', () => {
  beforeEach(() => {
    sinon.stub(OTLPMetricExporter.prototype, 'export').callsFake((_metrics, callback) => callback({ code: 0 }));
  });
  afterEach(() => sinon.restore());

  it('should not rebuild the meter when the settings are satisfied', async () => {
    const manager = new ClientMeterManager('client-1', () => new Metadata(), createSilentLogger());
    try {
      await manager.reset(metricOf(true));
      const meter = currentMeter(manager);
      assert.equal(meter.enabled, true);
      await manager.reset(metricOf(true));
      assert.equal(currentMeter(manager), meter);
    } finally {
      await manager.shutdown();
    }
  });

  it('should shut down the previous meter when the endpoints change', async () => {
    const manager = new ClientMeterManager('client-1', () => new Metadata(), createSilentLogger());
    try {
      await manager.reset(metricOf(true, CLOSED_PORT));
      const previous = currentMeter(manager);
      const shutdown = sinon.spy(previous, 'shutdown');
      await manager.reset(metricOf(true, CLOSED_PORT - 1));
      sinon.assert.calledOnce(shutdown);
      assert.notEqual(currentMeter(manager), previous);
      assert.equal(manager.isEnabled(), true);
    } finally {
      await manager.shutdown();
    }
  });

  it('should shut down the meter and become disabled when the switch is turned off', async () => {
    const manager = new ClientMeterManager('client-1', () => new Metadata(), createSilentLogger());
    await manager.reset(metricOf(true));
    const shutdown = sinon.spy(currentMeter(manager), 'shutdown');
    await manager.reset(metricOf(false));
    sinon.assert.calledOnce(shutdown);
    assert.equal(manager.isEnabled(), false);
  });

  it('should observe the registered gauges when metrics are collected', async () => {
    const values = new Map<Attributes, number>([[{ topic: 'TopicA' }, 3 ]]);
    const observer: GaugeObserver = {
      getGauges: () => [ GaugeEnum.CONSUMER_CACHED_MESSAGES, GaugeEnum.CONSUMER_CACHED_BYTES ],
      getValues: sinon.stub<[GaugeEnum], Map<Attributes, number>>().returns(values),
    };
    const manager = new ClientMeterManager('client-1', () => new Metadata(), createSilentLogger());
    manager.setGaugeObserver(observer);
    await manager.reset(metricOf(true));
    // shutdown 会先 forceFlush，触发一次采集。
    await manager.shutdown();
    const getValues = observer.getValues as sinon.SinonStub;
    sinon.assert.calledWith(getValues, GaugeEnum.CONSUMER_CACHED_MESSAGES);
    sinon.assert.calledWith(getValues, GaugeEnum.CONSUMER_CACHED_BYTES);
  });

  it('should keep the meter installed by a reset that overlaps a turning-off reset', async () => {
    for (const nextPort of [ CLOSED_PORT, CLOSED_PORT - 1 ]) {
      const manager = new ClientMeterManager('client-1', () => new Metadata(), createSilentLogger());
      try {
        await manager.reset(metricOf(true, CLOSED_PORT));
        const first = currentMeter(manager);
        const gate = deferred<void>();
        const entered = deferred<void>();
        const realShutdown = first.shutdown.bind(first);
        const shutdown = sinon.stub(first, 'shutdown').callsFake(async () => {
          entered.resolve();
          await gate.promise;
          await realShutdown();
        });

        // 调用方（BaseClient#onSettingsCommand）不 await reset，两次 settings 可能交错。
        const off = manager.reset(metricOf(false));
        await entered.promise;
        const on = manager.reset(metricOf(true, nextPort));
        gate.resolve();
        await Promise.all([ off, on ]);

        assert.equal(manager.isEnabled(), true, `latest settings turn metrics on, port=${nextPort}`);
        assert.equal(currentMeter(manager).satisfy(metricOf(true, nextPort)), true);
        sinon.assert.calledOnce(shutdown);
      } finally {
        await manager.shutdown();
      }
    }
  });

  it('should let shutdown close the meter installed by an in-flight reset', async () => {
    const providerShutdown = sinon.spy(SdkMeterProvider.prototype, 'shutdown');
    const manager = new ClientMeterManager('client-1', () => new Metadata(), createSilentLogger());
    await manager.reset(metricOf(true, CLOSED_PORT));
    const first = currentMeter(manager);
    const gate = deferred<void>();
    const entered = deferred<void>();
    const realShutdown = first.shutdown.bind(first);
    sinon.stub(first, 'shutdown').callsFake(async () => {
      entered.resolve();
      await gate.promise;
      await realShutdown();
    });

    const off = manager.reset(metricOf(false));
    await entered.promise;
    const on = manager.reset(metricOf(true, CLOSED_PORT - 1));
    const closing = manager.shutdown();
    gate.resolve();
    await Promise.all([ off, on, closing ]);

    assert.equal(manager.isEnabled(), false);
    // 两个 provider 都被 shutdown；包括 shutdown 调用时尚未开始的开启型 reset。
    assert.equal(providerShutdown.callCount, 2);
  });

  it('should ignore settings after shutdown without constructing another provider', async () => {
    const manager = new ClientMeterManager('client-1', () => new Metadata(), createSilentLogger());
    await manager.reset(metricOf(true));
    const shutdown = sinon.spy(currentMeter(manager), 'shutdown');
    const getMeter = sinon.spy(SdkMeterProvider.prototype, 'getMeter');
    const closing = manager.shutdown();
    const lateReset = manager.reset(metricOf(true, CLOSED_PORT - 1));
    await Promise.all([ closing, lateReset, manager.shutdown() ]);
    await manager.reset(metricOf(true));
    sinon.assert.calledOnce(shutdown);
    sinon.assert.notCalled(getMeter);
    assert.equal(manager.isEnabled(), false);
  });

  it('should contain signing failures and retry successfully on the next collection', async () => {
    const metadataProvider = sinon.stub<[], Metadata>();
    metadataProvider.onFirstCall().throws(new Error('signing failed'));
    metadataProvider.returns(new Metadata());
    const manager = new ClientMeterManager('client-1', metadataProvider, createSilentLogger());
    try {
      await manager.reset(metricOf(true));
      manager.record(HistogramEnum.SEND_COST_TIME, {}, 3);
      const provider = (currentMeter(manager) as any).provider as SdkMeterProvider;
      await provider.forceFlush();
      assert.equal(manager.isEnabled(), true);
      await provider.forceFlush();
      sinon.assert.calledTwice(metadataProvider);
    } finally {
      await manager.shutdown();
    }
  });

  it('should finish collection when closing a temporary exporter fails', async () => {
    const shutdown = sinon.stub(OTLPMetricExporter.prototype, 'shutdown');
    shutdown.onFirstCall().rejects(new Error('exporter shutdown failed'));
    shutdown.resolves();
    const manager = new ClientMeterManager('client-1', () => new Metadata(), createSilentLogger());
    try {
      await manager.reset(metricOf(true));
      manager.record(HistogramEnum.SEND_COST_TIME, {}, 3);
      const provider = (currentMeter(manager) as any).provider as SdkMeterProvider;
      await provider.forceFlush();
      assert.equal(manager.isEnabled(), true);
    } finally {
      await manager.shutdown();
    }
  });

  it('should log invalid endpoint settings and accept the next valid settings', async () => {
    const logger = createSilentLogger();
    const manager = new ClientMeterManager('client-1', () => new Metadata(), logger);
    try {
      await manager.reset(new Metric(new MetricPB().setOn(true).setEndpoints(new EndpointsPB())));
      sinon.assert.calledOnce(logger.error);
      assert.equal(manager.isEnabled(), false);
      await manager.reset(metricOf(true));
      assert.equal(manager.isEnabled(), true);
    } finally {
      await manager.shutdown();
    }
  });
});

describe('ClientMeterManager OTLP export', () => {
  afterEach(() => sinon.restore());

  it('should export recorded metrics with the client metadata', async () => {
    const collector = await startCollector();
    const metadataProvider = sinon.spy(() => {
      const metadata = new Metadata();
      metadata.set('x-mq-client-id', 'client-export');
      return metadata;
    });
    const manager = new ClientMeterManager('client-export', metadataProvider, createSilentLogger());
    try {
      await manager.reset(metricOf(true, collector.port));
      manager.record(HistogramEnum.SEND_COST_TIME, { topic: 'TopicA' }, 3);
      // shutdown 会先 forceFlush，把已记录的指标导出一次。
      await manager.shutdown();
      assert.equal(collector.received.length, 1, 'expected one Export call at the collector');
      assert.deepEqual(collector.received[0].get('x-mq-client-id'), [ 'client-export' ]);
      sinon.assert.called(metadataProvider);
    } finally {
      await manager.shutdown();
      await collector.close();
    }
  });

  it('should sign each export again and close all temporary exporters', async () => {
    const collector = await startCollector();
    let sequence = 0;
    const metadataProvider = sinon.spy(() => {
      const metadata = new Metadata();
      metadata.set('x-mq-date-time', String(++sequence));
      return metadata;
    });
    const exporterShutdown = sinon.spy(OTLPMetricExporter.prototype, 'shutdown');
    const manager = new ClientMeterManager('client-export', metadataProvider, createSilentLogger());
    try {
      await manager.reset(metricOf(true, collector.port));
      manager.record(HistogramEnum.SEND_COST_TIME, {}, 3);
      const provider = (currentMeter(manager) as any).provider as SdkMeterProvider;
      await provider.forceFlush();
      await provider.forceFlush();
      await manager.shutdown();
      assert.deepEqual(collector.received.map(metadata => metadata.get('x-mq-date-time')), [[ '1' ], [ '2' ], [ '3' ]]);
      sinon.assert.calledThrice(metadataProvider);
      // 三个单轮 exporter，另加仅负责 aggregation 配置的 wrapper。
      assert.equal(exporterShutdown.callCount, 4);
    } finally {
      await manager.shutdown();
      await collector.close();
    }
  });

  it('should export through another endpoint when the first endpoint is unavailable', async () => {
    const unavailable = await startCollector();
    const collector = await startCollector();
    await unavailable.close();
    sinon.stub(Math, 'random').returns(0);
    const endpoints = new EndpointsPB().setScheme(AddressScheme.IPV4);
    endpoints.addAddresses().setHost('127.0.0.1').setPort(unavailable.port);
    endpoints.addAddresses().setHost('127.0.0.1').setPort(collector.port);
    const manager = new ClientMeterManager('client-export', () => new Metadata(), createSilentLogger());
    try {
      await manager.reset(new Metric(new MetricPB().setOn(true).setEndpoints(endpoints)));
      manager.record(HistogramEnum.SEND_COST_TIME, {}, 3);
      await manager.shutdown();
      assert.equal(collector.received.length, 1);
    } finally {
      await manager.shutdown();
      await collector.close();
    }
  });

  it('should export to IPv6 metric endpoints', async () => {
    const collector = await startCollector('::1');
    const logger = createSilentLogger();
    const manager = new ClientMeterManager('client-1', () => new Metadata(), logger);
    try {
      await manager.reset(metricOf(true, collector.port, '::1', AddressScheme.IPV6));
      sinon.assert.notCalled(logger.error);
      assert.equal(manager.isEnabled(), true);
      manager.record(HistogramEnum.SEND_COST_TIME, {}, 3);
      await manager.shutdown();
      assert.equal(collector.received.length, 1);
    } finally {
      await manager.shutdown();
      await collector.close();
    }
  });
});
