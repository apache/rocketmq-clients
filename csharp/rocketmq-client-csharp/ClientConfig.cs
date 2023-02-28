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
    public class ClientConfig : IClientConfig
    {
        private ClientConfig(ICredentialsProvider credentialsProvider, TimeSpan requestTimeout, string endpoints)
        {
            CredentialsProvider = credentialsProvider;
            RequestTimeout = requestTimeout;
            Endpoints = endpoints;
        }

        public ICredentialsProvider CredentialsProvider { get; }

        public TimeSpan RequestTimeout { get; }

        public string Endpoints { get; }

        public class Builder
        {
            private ICredentialsProvider _credentialsProvider;
            private TimeSpan _requestTimeout = TimeSpan.FromSeconds(3);
            private string _endpoints;

            public Builder SetCredentialsProvider(ICredentialsProvider credentialsProvider)
            {
                _credentialsProvider = credentialsProvider;
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

            public ClientConfig Build()
            {
                return new ClientConfig(_credentialsProvider, _requestTimeout, _endpoints);
            }
        }
    }
}