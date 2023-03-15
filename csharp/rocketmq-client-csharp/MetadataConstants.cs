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

using System.Reflection;

namespace Org.Apache.Rocketmq
{
    public class MetadataConstants
    {
        public const string NamespaceKey = "x-mq-namespace";
        public const string SessionTokenKey = "x-mq-session-token";
        public const string DateTimeKey = "x-mq-date-time";
        public const string LanguageKey = "x-mq-language";
        public const string ClientVersionKey = "x-mq-client-version";
        public const string ClientIdKey = "x-mq-client-id";
        public const string RequestIdKey = "x-mq-request-id";
        public const string ProtocolVersionKey = "x-mq-protocol-version";

        public const string Authorization = "authorization";
        public const string AlgorithmKey = "MQv2-HMAC-SHA1";
        public const string CredentialKey = "Credential";
        public const string SignedHeadersKey = "SignedHeaders";
        public const string SignatureKey = "Signature";
        public const string DateTimeFormat = "yyyyMMddTHHmmssZ";
        public const string LanguageValue = "DOTNET";

        private const string UnknownVersion = "unknown";

        public string ClientVersion { get; }


        public static readonly MetadataConstants Instance = new MetadataConstants();

        private MetadataConstants()
        {
            var assembly = Assembly.GetAssembly(typeof(MetadataConstants));
            if (null == assembly)
            {
                ClientVersion = UnknownVersion;
                return;
            }

            var assemblyInformationalVersionAttribute =
                assembly.GetCustomAttribute<AssemblyInformationalVersionAttribute>();
            if (null == assemblyInformationalVersionAttribute)
            {
                ClientVersion = UnknownVersion;
                return;
            }

            ClientVersion = assemblyInformationalVersionAttribute
                .InformationalVersion;
        }
    }
}