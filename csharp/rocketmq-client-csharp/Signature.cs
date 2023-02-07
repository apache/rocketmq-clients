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
using System.Text;
using grpc = Grpc.Core;
using System.Security.Cryptography;

namespace Org.Apache.Rocketmq
{
    public static class Signature
    {
        public static void Sign(IClient client, grpc::Metadata metadata)
        {
            var clientConfig = client.GetClientConfig();
            metadata.Add(MetadataConstants.LanguageKey, MetadataConstants.LanguageValue);
            metadata.Add(MetadataConstants.ClientVersionKey, MetadataConstants.Instance.ClientVersion);
            metadata.Add(MetadataConstants.ClientIdKey, client.GetClientId());

            var time = DateTime.Now.ToString(MetadataConstants.DateTimeFormat);
            metadata.Add(MetadataConstants.DateTimeKey, time);

            var credentials = clientConfig.CredentialsProvider?.Credentials;
            if (credentials == null || credentials.expired())
            {
                return;
            }

            if (!string.IsNullOrEmpty(credentials.SessionToken))
            {
                metadata.Add(MetadataConstants.SessionTokenKey, credentials.SessionToken);
            }

            var secretData = Encoding.ASCII.GetBytes(credentials.AccessSecret);
            var data = Encoding.ASCII.GetBytes(time);
            var signer = new HMACSHA1(secretData);
            var digest = signer.ComputeHash(data);
            var hmac = BitConverter.ToString(digest).Replace("-", "");
            var authorization = $"{MetadataConstants.AlgorithmKey} " +
                                $"{MetadataConstants.CredentialKey}={credentials.AccessKey}, " +
                                $"{MetadataConstants.SignedHeadersKey}={MetadataConstants.DateTimeKey}, " +
                                $"{MetadataConstants.SignatureKey}={hmac}";
            metadata.Add(MetadataConstants.Authorization, authorization);
        }
    }
}