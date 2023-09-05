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
using System.Diagnostics.Metrics;
using System.Net.Http;
using Microsoft.Extensions.Logging;
using OpenTelemetry;
using OpenTelemetry.Exporter;
using OpenTelemetry.Metrics;
using OpenTelemetry.Resources;

namespace Org.Apache.Rocketmq
{
    public class ClientMeterManager
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<ClientMeterManager>();
        private const string MeterName = "Apache.RocketMQ.Client";
        private const string Version = "1.0";
        private const int MetricExportPeriodInMillis = 60 * 1000;

        private readonly Client _client;
        private volatile ClientMeter _clientMeter;
        private readonly HttpClient _httpClient;
        internal readonly Meter Meter;

        public ClientMeterManager(Client client)
        {
            _client = client;
            var httpDelegatingHandler = new MetricHttpDelegatingHandler(client);
            _httpClient = new HttpClient(httpDelegatingHandler);
            _clientMeter = ClientMeter.DisabledInstance(_client.GetClientId());
            Meter = new Meter(MeterName, Version);
        }

        public void Shutdown()
        {
            _clientMeter.Shutdown();
        }

        public void Reset(Metric metric)
        {
            lock (this)
            {
                var clientId = _client.GetClientId();
                if (_clientMeter.Satisfy(metric))
                {
                    Logger.LogInformation(
                        $"Metric settings is satisfied by the current message meter, metric={metric}, clientId={clientId}");
                    return;
                }

                if (!metric.On)
                {
                    Logger.LogInformation($"Metric is off, clientId={clientId}");
                    _clientMeter.Shutdown();
                    _clientMeter = ClientMeter.DisabledInstance(clientId);
                    return;
                }

                var meterProvider = Sdk.CreateMeterProviderBuilder()
                    .SetResourceBuilder(ResourceBuilder.CreateEmpty())
                    .AddMeter(MeterName)
                    .AddOtlpExporter(delegate (OtlpExporterOptions options, MetricReaderOptions readerOptions)
                    {
                        options.Protocol = OtlpExportProtocol.Grpc;
                        options.Endpoint = new Uri(metric.Endpoints.GrpcTarget(_client.GetClientConfig().SslEnabled));
                        options.TimeoutMilliseconds = (int)_client.GetClientConfig().RequestTimeout.TotalMilliseconds;
                        options.HttpClientFactory = () => _httpClient;
                        readerOptions.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds =
                            MetricExportPeriodInMillis;
                    })
                    .AddView(instrument =>
                    {
                        if (MeterName != instrument.Meter.Name)
                        {
                            return null;
                        }

                        return instrument.Name switch
                        {
                            MetricConstant.SendCostTimeMetricName => MetricConstant.Instance.SendCostTimeBucket,
                            MetricConstant.DeliveryLatencyMetricName => MetricConstant.Instance.DeliveryLatencyBucket,
                            MetricConstant.AwaitTimeMetricName => MetricConstant.Instance.AwaitTimeBucket,
                            MetricConstant.ProcessTimeMetricName => MetricConstant.Instance.ProcessTimeBucket,
                            _ => null
                        };
                    })
                    .Build();

                var exist = _clientMeter;
                _clientMeter = new ClientMeter(metric.Endpoints, meterProvider, clientId);
                exist.Shutdown();
                Logger.LogInformation($"Metric is on, endpoints={metric.Endpoints}, clientId={clientId}");
            }
        }

        public bool IsEnabled()
        {
            return _clientMeter.Enabled;
        }
    }
}