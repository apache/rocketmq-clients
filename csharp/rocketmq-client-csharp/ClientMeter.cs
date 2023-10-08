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

using Microsoft.Extensions.Logging;
using OpenTelemetry.Metrics;

namespace Org.Apache.Rocketmq
{
    public class ClientMeter
    {
        private static readonly ILogger Logger = MqLogManager.CreateLogger<ClientMeter>();

        public ClientMeter(Endpoints endpoints, MeterProvider meterProvider, string clientId)
        {
            Enabled = true;
            Endpoints = endpoints;
            MeterProvider = meterProvider;
            ClientId = clientId;
        }

        private ClientMeter(string clientId)
        {
            Enabled = false;
            ClientId = clientId;
        }

        private Endpoints Endpoints { get; }

        private MeterProvider MeterProvider { get; }

        private string ClientId { get; }

        public bool Enabled { get; }

        internal static ClientMeter DisabledInstance(string clientId)
        {
            return new ClientMeter(clientId);
        }

        public void Shutdown()
        {
            if (!Enabled)
            {
                return;
            }

            Logger.LogInformation($"Begin to shutdown the client meter, clientId={ClientId}, endpoints={Endpoints}");
            MeterProvider.Shutdown();
            Logger.LogInformation($"Shutdown the client meter successfully, clientId={ClientId}, endpoints={Endpoints}");
        }

        public bool Satisfy(Metric metric)
        {
            if (Enabled && metric.On && Endpoints.Equals(metric.Endpoints))
            {
                return true;
            }

            return !Enabled && !metric.On;
        }
    }
}