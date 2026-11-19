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

import { Attributes, Meter } from '@opentelemetry/api';
import {
  MeterProvider as SdkMeterProvider,
  PeriodicExportingMetricReader,
  ResourceMetrics,
  View,
} from '@opentelemetry/sdk-metrics';
import { Resource } from '@opentelemetry/resources';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-grpc';
import { ChannelCredentials, Metadata } from '@grpc/grpc-js';
import { isIPv6 } from 'node:net';
import { ILogger } from '../client/Logger';
import { ClientMeter } from './ClientMeter';
import { GaugeObserver } from './GaugeObserver';
import { EmptyGaugeObserver } from './EmptyGaugeObserver';
import { HistogramEnum, buildHistogramView } from './HistogramEnum';
import { Metric } from './Metric';

const METRIC_EXPORTER_RPC_TIMEOUT = 5000;
const METRIC_READER_INTERVAL = 60000;
const METRIC_INSTRUMENTATION_NAME = 'org.apache.rocketmq.message';

type ExportCallback = Parameters<OTLPMetricExporter['export']>[1];
type ExportResult = Parameters<ExportCallback>[0];

// OTel 0.57.2 accepts a Metadata instance and caches it on the first export. Use a fresh exporter
// per round to refresh signatures and close its channel afterward; inherit aggregation/temporality choices.
class SignedMetricExporter extends OTLPMetricExporter {
  private readonly pending = new Set<Promise<void>>();
  private closed = false;

  constructor(private readonly urls: string[], private readonly metadataProvider: () => Metadata) {
    super({ url: urls[0], credentials: ChannelCredentials.createInsecure() });
  }

  override export(metrics: ResourceMetrics, callback: ExportCallback): void {
    if (this.closed) {
      callback({ code: 1, error: new Error('Metric exporter is shut down') });
      return;
    }
    const completion = this.exportOnce(metrics).then(callback, error => {
      callback({ code: 1, error: error instanceof Error ? error : new Error(String(error)) });
    });
    this.pending.add(completion);
    void completion.then(() => this.pending.delete(completion), () => this.pending.delete(completion));
  }

  private async exportOnce(metrics: ResourceMetrics): Promise<ExportResult> {
    const deadline = Date.now() + METRIC_EXPORTER_RPC_TIMEOUT;
    const start = Math.floor(Math.random() * this.urls.length);
    let result: ExportResult = { code: 1, error: new Error('No metric endpoint is available') };
    // Randomize the starting address and fail over as Java's multi-address resolver does; all attempts share one timeout.
    for (let index = 0; index < this.urls.length; index++) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) {
        break;
      }
      let exporter: OTLPMetricExporter | undefined;
      try {
        exporter = new OTLPMetricExporter({
          url: this.urls[(start + index) % this.urls.length],
          credentials: ChannelCredentials.createInsecure(),
          timeoutMillis: Math.max(1, Math.floor(remaining / (this.urls.length - index))),
          metadata: this.metadataProvider(),
        });
        result = await new Promise<ExportResult>(resolve => exporter!.export(metrics, resolve));
      } catch (error) {
        result = { code: 1, error: error instanceof Error ? error : new Error(String(error)) };
      } finally {
        await exporter?.shutdown();
      }
      if (result.code === 0) {
        break;
      }
    }
    return result;
  }

  override async forceFlush(): Promise<void> {
    await Promise.all(this.pending);
  }

  override async shutdown(): Promise<void> {
    this.closed = true;
    await this.forceFlush();
    await super.shutdown();
  }
}

/**
 * Owns the OpenTelemetry metric pipeline for a single client. On every settings
 * command it lazily (re)builds an SdkMeterProvider that exports the four
 * RocketMQ histograms plus the consumer gauges to the broker-supplied metric
 * endpoints over OTLP/gRPC, mirroring
 * org.apache.rocketmq.client.java.metrics.ClientMeterManager.
 */
export class ClientMeterManager {
  private readonly clientId: string;
  private readonly metadataProvider: () => Metadata;
  private readonly logger: ILogger;
  private clientMeter: ClientMeter;
  private gaugeObserver: GaugeObserver = EmptyGaugeObserver.EMPTY;
  private pending: Promise<void> = Promise.resolve();
  private shutdownPromise?: Promise<void>;

  constructor(clientId: string, metadataProvider: () => Metadata, logger: ILogger) {
    this.clientId = clientId;
    this.metadataProvider = metadataProvider;
    this.logger = logger;
    this.clientMeter = ClientMeter.disabledInstance();
  }

  setGaugeObserver(gaugeObserver: GaugeObserver) {
    this.gaugeObserver = gaugeObserver;
  }

  record(histogramEnum: HistogramEnum, attributes: Attributes, value: number) {
    this.clientMeter.record(histogramEnum, attributes, value);
  }

  isEnabled(): boolean {
    return this.clientMeter.enabled;
  }

  shutdown(): Promise<void> {
    this.shutdownPromise ??= this.enqueue(async () => {
      const meter = this.clientMeter;
      this.clientMeter = ClientMeter.disabledInstance();
      await meter.shutdown();
    });
    return this.shutdownPromise;
  }

  reset(metric: Metric): Promise<void> {
    if (this.shutdownPromise) {
      return this.shutdownPromise;
    }
    return this.enqueue(() => this.resetMeter(metric));
  }

  // Settings handlers do not await reset; serialize disable/enable and shutdown in invocation order.
  private enqueue(operation: () => Promise<void>): Promise<void> {
    const result = this.pending.then(operation);
    this.pending = result.catch(() => undefined);
    return result;
  }

  private async resetMeter(metric: Metric): Promise<void> {
    try {
      if (this.clientMeter.satisfy(metric)) {
        this.logger.info('Metric settings is satisfied by the current message meter, metric=%j, clientId=%s',
          metric, this.clientId);
        return;
      }
      if (!metric.on || !metric.endpoints) {
        this.logger.info('Metric is off, clientId=%s', this.clientId);
        await this.clientMeter.shutdown();
        this.clientMeter = ClientMeter.disabledInstance();
        return;
      }
      const endpoints = metric.endpoints;
      const urls = endpoints.addressesList.map(address => {
        const host = isIPv6(address.host) ? `[${address.host}]` : address.host;
        return `http://${host}:${address.port}`;
      });
      if (urls.length === 0) {
        throw new Error('Metric endpoints must contain at least one address');
      }
      const exporter = new SignedMetricExporter(urls, this.metadataProvider);

      const reader = new PeriodicExportingMetricReader({
        exporter,
        exportIntervalMillis: METRIC_READER_INTERVAL,
        exportTimeoutMillis: METRIC_EXPORTER_RPC_TIMEOUT,
      });

      const views: View[] = [
        new View(buildHistogramView(HistogramEnum.SEND_COST_TIME)),
        new View(buildHistogramView(HistogramEnum.DELIVERY_LATENCY)),
        new View(buildHistogramView(HistogramEnum.AWAIT_TIME)),
        new View(buildHistogramView(HistogramEnum.PROCESS_TIME)),
      ];

      const provider: SdkMeterProvider = new SdkMeterProvider({
        resource: Resource.empty(),
        readers: [ reader ],
        views,
      });

      const meter: Meter = provider.getMeter(METRIC_INSTRUMENTATION_NAME);

      const existedClientMeter = this.clientMeter;
      this.clientMeter = new ClientMeter(meter, endpoints, provider);
      await existedClientMeter.shutdown();
      this.logger.info('Metrics is on, endpoints=%s, clientId=%s', endpoints, this.clientId);

      const gauges = this.gaugeObserver.getGauges();
      for (const gauge of gauges) {
        meter.createObservableGauge(gauge).addCallback(observableResult => {
          const values = this.gaugeObserver.getValues(gauge);
          for (const [ attributes, value ] of values) {
            observableResult.observe(value, attributes);
          }
        });
      }
    } catch (t) {
      this.logger.error('Exception raised when resetting message meter, clientId=%s, error=%s', this.clientId, t);
    }
  }
}
