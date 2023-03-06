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
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public abstract class Settings
    {
        protected readonly string ClientId;
        protected readonly ClientType ClientType;
        protected readonly Endpoints Endpoints;
        protected volatile IRetryPolicy RetryPolicy;
        protected readonly TimeSpan RequestTimeout;

        protected Settings(string clientId, ClientType clientType, Endpoints endpoints, IRetryPolicy retryPolicy,
            TimeSpan requestTimeout)
        {
            ClientId = clientId;
            ClientType = clientType;
            Endpoints = endpoints;
            RetryPolicy = retryPolicy;
            RequestTimeout = requestTimeout;
        }

        protected Settings(string clientId, ClientType clientType, Endpoints endpoints, TimeSpan requestTimeout)
        {
            ClientId = clientId;
            ClientType = clientType;
            Endpoints = endpoints;
            RetryPolicy = null;
            RequestTimeout = requestTimeout;
        }

        public abstract Proto::Settings ToProtobuf();

        public abstract void Sync(Proto::Settings settings);

        public IRetryPolicy GetRetryPolicy()
        {
            return RetryPolicy;
        }
    }
}