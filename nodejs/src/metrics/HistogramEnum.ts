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

import { ExplicitBucketHistogramAggregation, InstrumentType, ViewOptions } from '@opentelemetry/sdk-metrics';

/**
 * The four histograms exported by the RocketMQ client, mirroring
 * org.apache.rocketmq.client.java.metrics.HistogramEnum. The bucket
 * boundaries (in milliseconds) are copied verbatim from the Java client.
 */
export enum HistogramEnum {
  /**
   * Cost time of successful api calls of message publishing.
   * Labels: topic, client_id, invocation_status.
   */
  SEND_COST_TIME = 'rocketmq_send_cost_time',
  /**
   * Latency of message delivery from remote.
   * Labels: topic, client_id, consumer_group.
   */
  DELIVERY_LATENCY = 'rocketmq_delivery_latency',
  /**
   * Await time of message consumption.
   * Labels: topic, client_id, consumer_group.
   */
  AWAIT_TIME = 'rocketmq_await_time',
  /**
   * Process time of message consumption.
   * Labels: topic, client_id, consumer_group, invocation_status.
   */
  PROCESS_TIME = 'rocketmq_process_time',
}

/**
 * Explicit bucket boundaries (milliseconds) for each histogram, identical to
 * the Java client's Aggregation.explicitBucketHistogram(...) calls.
 */
export const HISTOGRAM_BUCKETS: Record<HistogramEnum, number[]> = {
  [HistogramEnum.SEND_COST_TIME]: [ 1.0, 5.0, 10.0, 20.0, 50.0, 200.0, 500.0 ],
  [HistogramEnum.DELIVERY_LATENCY]: [ 1.0, 5.0, 10.0, 20.0, 50.0, 200.0, 500.0 ],
  [HistogramEnum.AWAIT_TIME]: [ 1.0, 5.0, 20.0, 100.0, 1000.0, 5 * 1000.0, 10 * 1000.0 ],
  [HistogramEnum.PROCESS_TIME]: [ 1.0, 5.0, 10.0, 100.0, 1000.0, 10 * 1000.0, 60 * 1000.0 ],
};

/**
 * Build the OpenTelemetry view that forces the given histogram onto the exact
 * explicit-bucket aggregation used by the Java client.
 */
export function buildHistogramView(histogramEnum: HistogramEnum): ViewOptions {
  return {
    instrumentName: histogramEnum,
    instrumentType: InstrumentType.HISTOGRAM,
    aggregation: new ExplicitBucketHistogramAggregation(HISTOGRAM_BUCKETS[histogramEnum]),
  };
}
