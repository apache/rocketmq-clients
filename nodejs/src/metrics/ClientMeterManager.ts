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
  View,
} from '@opentelemetry/sdk-metrics';
import { Resource } from '@opentelemetry/resources';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-grpc';
import { ChannelCredentials, Metadata } from '@grpc/grpc-js';
import { ILogger } from '../client/Logger';
import { ClientMeter } from './ClientMeter';
import { GaugeObserver } from './GaugeObserver';
import { EmptyGaugeObserver } from './EmptyGaugeObserver';
import { HistogramEnum, buildHistogramView } from './HistogramEnum';
import { Metric } from './Metric';

const METRIC_EXPORTER_RPC_TIMEOUT = 5000;
const METRIC_READER_INTERVAL = 60000;
const METRIC_INSTRUMENTATION_NAME = 'org.apache.rocketmq.message';

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

  async shutdown() {
    await this.clientMeter.shutdown();
  }

  async reset(metric: Metric): Promise<void> {
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
      // Use the first address as the OTLP/gRPC target. The exporter's URL is
      // normalized to "host:port" internally, which gRPC-js resolves through
      // its default DNS resolver (a single IP works fine; the custom `ip`
      // resolver is used for the primary RPC channels).
      const address = endpoints.addressesList[0];
      const url = `http://${address.host}:${address.port}`;
      const exporter = new OTLPMetricExporter({
        url,
        credentials: ChannelCredentials.createInsecure(),
        timeoutMillis: METRIC_EXPORTER_RPC_TIMEOUT,
        // Per-request signing mirrors the Java client's AuthInterceptor. The
        // d.ts types this field as `Metadata`, but the runtime (and the
        // exporter's own merge logic in otlp-grpc-exporter-base) expects a
        // function returning freshly-signed Metadata.
        metadata: (() => this.metadataProvider()) as unknown as Metadata,
      });

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
