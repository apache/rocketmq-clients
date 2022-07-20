/*
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

using System;
using System.Threading.Tasks;
using rmq = Apache.Rocketmq.V2;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using NLog;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;

namespace Org.Apache.Rocketmq
{
    public class Producer : Client, IProducer
    {
        public Producer(AccessPoint accessPoint, string resourceNamespace) : base(accessPoint, resourceNamespace)
        {
            _loadBalancer = new ConcurrentDictionary<string, PublishLoadBalancer>();
            _sendFailureTotal = MetricMeter.CreateCounter<long>("rocketmq_send_failure_total");
            _sendLatency = MetricMeter.CreateHistogram<double>(SendLatencyName, 
                description: "Measure the duration of publishing messages to brokers",
                unit: "milliseconds");
        }

        public override async Task Start()
        {
            await base.Start();
            // More initialization
            // TODO: Add authentication header

            _meterProvider = Sdk.CreateMeterProviderBuilder()
                .AddMeter("Apache.RocketMQ.Client")
                .AddOtlpExporter(delegate(OtlpExporterOptions options, MetricReaderOptions readerOptions)
                {
                    options.Protocol = OtlpExportProtocol.Grpc;
                    options.Endpoint = new Uri(_accessPoint.TargetUrl());
                    options.TimeoutMilliseconds = (int) _clientSettings.RequestTimeout.ToTimeSpan().TotalMilliseconds;

                    readerOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = 60 * 1000;
                })
                .AddView((instrument) =>
                {
                    if (instrument.Meter.Name == MeterName && instrument.Name == SendLatencyName)
                    {
                        return new ExplicitBucketHistogramConfiguration()
                        {
                            Boundaries = new double[] {1, 5, 10, 20, 50, 200, 500},
                        };
                    }
                    return null;
                })
                .Build();
        }

        public override async Task Shutdown()
        {
            // Release local resources
            await base.Shutdown();
        }

        protected override void PrepareHeartbeatData(rmq::HeartbeatRequest request)
        {
            request.ClientType = rmq::ClientType.Producer;

            // Concept of ProducerGroup has been removed.
        }

        public async Task<SendReceipt> Send(Message message)
        {
            if (!_loadBalancer.ContainsKey(message.Topic))
            {
                var topicRouteData = await GetRouteFor(message.Topic, false);
                if (null == topicRouteData || null == topicRouteData.MessageQueues || 0 == topicRouteData.MessageQueues.Count)
                {
                    Logger.Error($"Failed to resolve route info for {message.Topic}");
                    throw new TopicRouteException(string.Format("No topic route for {0}", message.Topic));
                }

                var loadBalancerItem = new PublishLoadBalancer(topicRouteData);
                _loadBalancer.TryAdd(message.Topic, loadBalancerItem);
            }

            var publishLb = _loadBalancer[message.Topic];

            var request = new rmq::SendMessageRequest();
            var entry = new rmq::Message();
            entry.Body = ByteString.CopyFrom(message.Body);
            entry.Topic = new rmq::Resource();
            entry.Topic.ResourceNamespace = resourceNamespace();
            entry.Topic.Name = message.Topic;
            request.Messages.Add(entry);

            // User properties
            foreach (var item in message.UserProperties)
            {
                entry.UserProperties.Add(item.Key, item.Value);
            }

            entry.SystemProperties = new rmq::SystemProperties();
            entry.SystemProperties.MessageId = message.MessageId;
            entry.SystemProperties.MessageType = rmq::MessageType.Normal;
            if (DateTime.MinValue != message.DeliveryTimestamp)
            {
                entry.SystemProperties.MessageType = rmq::MessageType.Delay;
                entry.SystemProperties.DeliveryTimestamp = Timestamp.FromDateTime(message.DeliveryTimestamp);

                if (message.Fifo())
                {
                    Logger.Warn("A message may not be FIFO and delayed at the same time");
                    throw new MessageException("A message may not be both FIFO and Timed");
                }
            } else if (!String.IsNullOrEmpty(message.MessageGroup))
            {
                entry.SystemProperties.MessageType = rmq::MessageType.Fifo;
                entry.SystemProperties.MessageGroup = message.MessageGroup;
            }
            
            if (!string.IsNullOrEmpty(message.Tag))
            {
                entry.SystemProperties.Tag = message.Tag;
            }

            if (0 != message.Keys.Count)
            {
                foreach (var key in message.Keys)
                {
                    entry.SystemProperties.Keys.Add(key);
                }
            }

            List<string> targets = new List<string>();
            List<rmq::MessageQueue> candidates = publishLb.Select(message.MaxAttemptTimes);
            foreach (var messageQueue in candidates)
            {
                targets.Add(Utilities.TargetUrl(messageQueue));
            }

            var metadata = new Metadata();
            Signature.sign(this, metadata);

            Exception ex = null;

            foreach (var target in targets)
            {
                try
                {
                    var stopWatch = new Stopwatch();
                    stopWatch.Start();
                    rmq::SendMessageResponse response = await Manager.SendMessage(target, metadata, request, RequestTimeout);
                    if (null != response && rmq::Code.Ok == response.Status.Code)
                    {
                        var messageId = response.Entries[0].MessageId;
                        
                        // Account latency histogram
                        stopWatch.Stop();
                        var latency = stopWatch.ElapsedMilliseconds;
                        _sendLatency.Record(latency, new("topic", message.Topic), new("client_id", clientId()));
                        
                        return new SendReceipt(messageId);
                    }
                }
                catch (Exception e)
                {
                    // Account failure count
                    _sendFailureTotal.Add(1, new("topic", message.Topic), new("client_id", clientId()));                    
                    Logger.Info(e, $"Failed to send message to {target}");
                    ex = e;
                }
            }

            if (null != ex)
            {
                Logger.Error(ex, $"Failed to send message after {message.MaxAttemptTimes} attempts");
                throw ex;
            }

            Logger.Error($"Failed to send message after {message.MaxAttemptTimes} attempts with unspecified reasons");
            throw new Exception("Send message failed");
        }

        private readonly ConcurrentDictionary<string, PublishLoadBalancer> _loadBalancer;

        private readonly Counter<long> _sendFailureTotal;
        private readonly Histogram<double> _sendLatency;

        private static readonly string SendLatencyName = "rocketmq_send_success_cost_time";
        private MeterProvider _meterProvider;
    }
}