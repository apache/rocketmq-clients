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
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;

namespace Org.Apache.Rocketmq
{
    public class Producer : Client, IProducer
    {
        public Producer(string accessUrl) : base(accessUrl)
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

            _meterProvider = Sdk.CreateMeterProviderBuilder()
                .AddMeter("Apache.RocketMQ.Client")
                .AddOtlpExporter(delegate(OtlpExporterOptions options, MetricReaderOptions readerOptions)
                {
                    options.Protocol = OtlpExportProtocol.Grpc;
                    options.Endpoint = new Uri(AccessPoint.TargetUrl());
                    options.TimeoutMilliseconds = (int) ClientSettings.RequestTimeout.ToTimeSpan().TotalMilliseconds;

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

        public override void BuildClientSetting(rmq::Settings settings)
        {
            base.BuildClientSetting(settings);

            settings.ClientType = rmq.ClientType.Producer;
            var publishing = new rmq.Publishing();
            
            foreach (var topic in _topicsOfInterest)
            {
                var resource = new rmq.Resource()
                {
                    Name = topic.Key,
                    ResourceNamespace = ResourceNamespace
                };
                publishing.Topics.Add(resource);
            }

            settings.Publishing = publishing;
        }

        public async Task<SendReceipt> Send(Message message)
        {
            _topicsOfInterest.TryAdd(message.Topic, true);

            if (!_loadBalancer.TryGetValue(message.Topic, out var publishLb))
            {
                var topicRouteData = await GetRouteFor(message.Topic, false);
                if (null == topicRouteData || null == topicRouteData.MessageQueues || 0 == topicRouteData.MessageQueues.Count)
                {
                    Logger.Error($"Failed to resolve route info for {message.Topic}");
                    throw new TopicRouteException(string.Format("No topic route for {0}", message.Topic));
                }

                publishLb = new PublishLoadBalancer(topicRouteData);
                _loadBalancer.TryAdd(message.Topic, publishLb);
            }

            var request = new rmq::SendMessageRequest();
            var entry = new rmq::Message
            {
                Body = ByteString.CopyFrom(message.Body),
                Topic = new rmq::Resource
                {
                    ResourceNamespace = resourceNamespace(),
                    Name = message.Topic
                },
                UserProperties = { message.UserProperties },
                SystemProperties = new rmq::SystemProperties
                {
                    MessageId = message.MessageId,
                    MessageType = rmq::MessageType.Normal,
                    Keys = { message.Keys },
                },
            };
            request.Messages.Add(entry);

            if (DateTime.MinValue != message.DeliveryTimestamp)
            {
                entry.SystemProperties.MessageType = rmq::MessageType.Delay;
                entry.SystemProperties.DeliveryTimestamp = Timestamp.FromDateTime(message.DeliveryTimestamp);

                if (message.Fifo())
                {
                    Logger.Warn("A message may not be FIFO and delayed at the same time");
                    throw new MessageException("A message may not be both FIFO and Timed");
                }
            }
            else if (!String.IsNullOrEmpty(message.MessageGroup))
            {
                entry.SystemProperties.MessageType = rmq::MessageType.Fifo;
                entry.SystemProperties.MessageGroup = message.MessageGroup;
            }

            if (!string.IsNullOrEmpty(message.Tag))
            {
                entry.SystemProperties.Tag = message.Tag;
            }

            var metadata = new Metadata();
            Signature.Sign(this, metadata);

            Exception ex = null;

            var candidates = publishLb.Select(message.MessageGroup, message.MaxAttemptTimes);
            foreach (var messageQueue in candidates)
            {
                var target = Utilities.TargetUrl(messageQueue);
                entry.SystemProperties.BornTimestamp = Timestamp.FromDateTimeOffset(DateTimeOffset.UtcNow);
                entry.SystemProperties.QueueId = messageQueue.Id;

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
                    else if (response?.Status is not null)
                    {
                        Logger.Warn($"Send failed with code: {response.Status.Code}, error: {response.Status.Message}");
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