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

import { Endpoints } from '../route';
import { Metric as MetricPB } from '../../proto/apache/rocketmq/v2/definition_pb';

/**
 * Local view of the metric switch received from the broker via the telemetry
 * settings command, mirroring org.apache.rocketmq.client.java.metrics.Metric.
 *
 * Metrics are only considered enabled when both the `on` flag is set AND a
 * valid endpoints is present.
 */
export class Metric {
  readonly endpoints: Endpoints | null;
  readonly on: boolean;

  constructor(metric: MetricPB | undefined | null) {
    if (metric && metric.hasEndpoints() && metric.getOn()) {
      // Endpoints expects the plain AsObject shape, so unwrap the proto message.
      this.endpoints = new Endpoints(metric.getEndpoints()!.toObject());
      this.on = true;
    } else {
      this.endpoints = null;
      this.on = false;
    }
  }
}
