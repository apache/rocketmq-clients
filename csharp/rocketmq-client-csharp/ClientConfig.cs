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
using System.Threading;

namespace Org.Apache.Rocketmq
{
    public class ClientConfig : IClientConfig
    {
        private static long _instanceSequence = 0;

        public ClientConfig(string endpoints)
        {
            var hostName = System.Net.Dns.GetHostName();
            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
            ClientId = $"{hostName}@{pid}@{Interlocked.Increment(ref _instanceSequence)}";
            RequestTimeout = TimeSpan.FromSeconds(3);
            Endpoints = new Endpoints(endpoints);
        }

        public ICredentialsProvider CredentialsProvider { get; set; }

        public TimeSpan RequestTimeout { get; set; }

        public string ClientId { get; }


        public Endpoints Endpoints { get; }
    }
}