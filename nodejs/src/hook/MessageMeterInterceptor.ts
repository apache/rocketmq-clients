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
import { Attribute, AttributeKey, MessageHookPoints, MessageHookPointsStatus, MessageInterceptor, MessageInterceptorContext } from './';
import { GeneralMessage } from './MessageInterceptor';
import { MetricLabels } from '../metrics/MetricLabels';
import { InvocationStatus } from '../metrics/InvocationStatus';
import { HistogramEnum } from '../metrics/HistogramEnum';
import { ClientMeterManager } from '../metrics/ClientMeterManager';

/**
 * Minimal message shape required to compute metric attributes / latencies.
 * Covers both the producer's PublishingMessage (cast to GeneralMessage) and the
 * consumer's MessageView (which implements GeneralMessage).
 */
type MetricMessage = {
  topic: string;
  transportDeliveryTimestamp?: Date;
  decodeTimestamp?: Date;
};

/**
 * Records the four RocketMQ histograms around the SEND / RECEIVE / CONSUME hook
 * points, mirroring org.apache.rocketmq.client.java.metrics.MessageMeterInterceptor.
 *
 * Unlike Java (which wires an AuthInterceptor + IpNameResolverFactory onto the
 * export channel), the Node client signs each export request via a per-call
 * metadata provider (see {@link ClientMeterManager}), so no auth plumbing is
 * needed here.
 */
export class MessageMeterInterceptor implements MessageInterceptor {
  static readonly SEND_STOPWATCH_KEY = AttributeKey.create<number>('send_stopwatch');
  static readonly CONSUME_STOPWATCH_KEY = AttributeKey.create<number>('consume_stopwatch');

  constructor(
    private readonly meterManager: ClientMeterManager,
    private readonly clientId: string,
    private readonly getConsumerGroup: () => string | undefined,
  ) {
  }

  doBefore(context: MessageInterceptorContext, messages: GeneralMessage[]): void {
    if (!this.meterManager.isEnabled()) {
      return;
    }
    switch (context.getMessageHookPoints()) {
      case MessageHookPoints.SEND:
        context.putAttribute(MessageMeterInterceptor.SEND_STOPWATCH_KEY, Attribute.create(Date.now()));
        break;
      case MessageHookPoints.CONSUME: {
        const consumerGroup = this.getConsumerGroup();
        if (!consumerGroup) {
          break;
        }
        const message = messages[0] as MetricMessage | undefined;
        if (!message || !message.decodeTimestamp) {
          break;
        }
        const latency = Date.now() - message.decodeTimestamp.getTime();
        const attributes = this.#consumerAttributes(message.topic, consumerGroup);
        this.meterManager.record(HistogramEnum.AWAIT_TIME, attributes, latency);
        context.putAttribute(MessageMeterInterceptor.CONSUME_STOPWATCH_KEY, Attribute.create(Date.now()));
        break;
      }
      default:
        break;
    }
  }

  doAfter(context: MessageInterceptorContext, messages: GeneralMessage[]): void {
    if (!this.meterManager.isEnabled()) {
      return;
    }
    const status = context.getStatus();
    const invocationStatus = status === MessageHookPointsStatus.OK ? InvocationStatus.SUCCESS : InvocationStatus.FAILURE;
    switch (context.getMessageHookPoints()) {
      case MessageHookPoints.SEND: {
        const attr = context.getAttribute(MessageMeterInterceptor.SEND_STOPWATCH_KEY);
        if (!attr) {
          break;
        }
        const message = messages[0] as MetricMessage | undefined;
        if (!message) {
          break;
        }
        const cost = Date.now() - attr.get();
        const attributes: Attributes = {
          [MetricLabels.TOPIC]: message.topic,
          [MetricLabels.CLIENT_ID]: this.clientId,
          [MetricLabels.INVOCATION_STATUS]: invocationStatus.name,
        };
        this.meterManager.record(HistogramEnum.SEND_COST_TIME, attributes, cost);
        break;
      }
      case MessageHookPoints.RECEIVE: {
        const consumerGroup = this.getConsumerGroup();
        if (!consumerGroup) {
          break;
        }
        const message = messages[0] as MetricMessage | undefined;
        if (!message || !message.transportDeliveryTimestamp) {
          break;
        }
        const latency = Date.now() - message.transportDeliveryTimestamp.getTime();
        if (latency < 0) {
          break;
        }
        const attributes = this.#consumerAttributes(message.topic, consumerGroup);
        this.meterManager.record(HistogramEnum.DELIVERY_LATENCY, attributes, latency);
        break;
      }
      case MessageHookPoints.CONSUME: {
        const consumerGroup = this.getConsumerGroup();
        if (!consumerGroup) {
          break;
        }
        const attr = context.getAttribute(MessageMeterInterceptor.CONSUME_STOPWATCH_KEY);
        if (!attr) {
          break;
        }
        const message = messages[0] as MetricMessage | undefined;
        if (!message) {
          break;
        }
        const cost = Date.now() - attr.get();
        const attributes: Attributes = {
          [MetricLabels.TOPIC]: message.topic,
          [MetricLabels.CLIENT_ID]: this.clientId,
          [MetricLabels.CONSUMER_GROUP]: consumerGroup,
          [MetricLabels.INVOCATION_STATUS]: invocationStatus.name,
        };
        this.meterManager.record(HistogramEnum.PROCESS_TIME, attributes, cost);
        break;
      }
      default:
        break;
    }
  }

  #consumerAttributes(topic: string, consumerGroup: string): Attributes {
    return {
      [MetricLabels.TOPIC]: topic,
      [MetricLabels.CLIENT_ID]: this.clientId,
      [MetricLabels.CONSUMER_GROUP]: consumerGroup,
    };
  }
}
