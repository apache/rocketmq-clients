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
    public class MetadataConstants
    {
        public const string TENANT_ID_KEY = "x-mq-tenant-id";
        public const string NAMESPACE_KEY = "x-mq-namespace";
        public const string AUTHORIZATION = "authorization";
        public const string STS_SESSION_TOKEN = "x-mq-session-token";
        public const string DATE_TIME_KEY = "x-mq-date-time";
        public const string ALGORITHM_KEY = "MQv2-HMAC-SHA1";
        public const string CREDENTIAL_KEY = "Credential";
        public const string SIGNED_HEADERS_KEY = "SignedHeaders";
        public const string SIGNATURE_KEY = "Signature";
        public const string DATE_TIME_FORMAT = "yyyyMMddTHHmmssZ";
        public const string LANGUAGE_KEY = "x-mq-language";
        public const string CLIENT_VERSION_KEY = "x-mq-client-version";
        public const string PROTOCOL_VERSION_KEY = "x-mq-protocol-version";
        public const string REQUEST_ID_KEY = "x-mq-request-id";

        public const string CLIENT_ID_KEY = "x-mq-client-id";
    }
}