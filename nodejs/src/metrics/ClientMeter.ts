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

import { Attributes, Histogram, Meter } from '@opentelemetry/api';
import { MeterProvider as SdkMeterProvider } from '@opentelemetry/sdk-metrics';
import { Endpoints } from '../route';
import { HistogramEnum } from './HistogramEnum';
import { Metric } from './Metric';

/**
 * Wraps an OpenTelemetry {@link Meter} and records the four RocketMQ histograms.
 * When metrics are disabled it is a no-op instance, mirroring
 * org.apache.rocketmq.client.java.metrics.ClientMeter.
 */
export class ClientMeter {
  readonly enabled: boolean;
  private readonly meter?: Meter;
  private readonly endpoints?: Endpoints;
  private readonly provider?: SdkMeterProvider;
  private readonly histogramMap = new Map<HistogramEnum, Histogram>();

  constructor(meter: Meter | null, endpoints: Endpoints | null, provider: SdkMeterProvider | null) {
    this.enabled = meter != null && endpoints != null && provider != null;
    this.meter = meter ?? undefined;
    this.endpoints = endpoints ?? undefined;
    this.provider = provider ?? undefined;
  }

  static disabledInstance(): ClientMeter {
    return new ClientMeter(null, null, null);
  }

  record(histogramEnum: HistogramEnum, attributes: Attributes, value: number) {
    if (!this.enabled || !this.meter) {
      return;
    }
    let histogram = this.histogramMap.get(histogramEnum);
    if (!histogram) {
      histogram = this.meter.createHistogram(histogramEnum);
      this.histogramMap.set(histogramEnum, histogram);
    }
    histogram.record(value, attributes);
  }

  async shutdown() {
    if (!this.enabled || !this.provider) {
      return;
    }
    try {
      await this.provider.shutdown();
    } catch (t) {
      // best-effort shutdown, ignore
    }
  }

  /**
   * Returns true if the supplied metric switch is already satisfied by this
   * meter (so a rebuild is unnecessary), mirroring ClientMeter#satisfy.
   */
  satisfy(metric: Metric): boolean {
    if (this.enabled && metric.on && this.endpoints != null && metric.endpoints != null
      && this.endpoints.equals(metric.endpoints)) {
      return true;
    }
    return !this.enabled && !metric.on;
  }
}
