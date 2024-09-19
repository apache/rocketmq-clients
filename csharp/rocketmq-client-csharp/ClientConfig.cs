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

namespace Org.Apache.Rocketmq
{
    public class ClientConfig
    {
        private ClientConfig(ISessionCredentialsProvider sessionCredentialsProvider, TimeSpan requestTimeout,
            string endpoints, bool sslEnabled, string namespaceName)
        {
            SessionCredentialsProvider = sessionCredentialsProvider;
            RequestTimeout = requestTimeout;
            Endpoints = endpoints;
            SslEnabled = sslEnabled;
            Namespace = namespaceName;
        }

        public ISessionCredentialsProvider SessionCredentialsProvider { get; }

        public TimeSpan RequestTimeout { get; }

        public string Endpoints { get; }

        public bool SslEnabled { get; }

        public string Namespace { get; }

        public class Builder
        {
            private ISessionCredentialsProvider _sessionCredentialsProvider;
            private TimeSpan _requestTimeout = TimeSpan.FromSeconds(3);
            private string _endpoints;
            private bool _sslEnabled = true;
            private string _namespace = "";

            public Builder SetCredentialsProvider(ISessionCredentialsProvider sessionCredentialsProvider)
            {
                _sessionCredentialsProvider = sessionCredentialsProvider;
                return this;
            }

            public Builder SetRequestTimeout(TimeSpan requestTimeout)
            {
                _requestTimeout = requestTimeout;
                return this;
            }

            public Builder SetEndpoints(string endpoints)
            {
                _endpoints = endpoints;
                return this;
            }

            public Builder EnableSsl(bool sslEnabled)
            {
                _sslEnabled = sslEnabled;
                return this;
            }

            public Builder SetNamespace(string namespaceName)
            {
                _namespace = namespaceName;
                return this;
            }

            public ClientConfig Build()
            {
                return new ClientConfig(_sessionCredentialsProvider, _requestTimeout, _endpoints, _sslEnabled, _namespace);
            }
        }
    }
}