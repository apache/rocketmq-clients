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
using System.Collections.Generic;
using System.Text;
using grpcLib = Grpc.Core;
using System.Security.Cryptography;

namespace Org.Apache.Rocketmq
{
    public static class Signature
    {
        public static void Sign(Client client, grpcLib::Metadata metadata)
        {
            var headers = Sign(client);
            foreach (var (key, value) in headers)
            {
                metadata.Add(key, value);
            }
        }

        public static Dictionary<string, string> Sign(Client client)
        {
            Dictionary<string, string> dictionary = new Dictionary<string, string>();
            var clientConfig = client.GetClientConfig();
            dictionary.Add(MetadataConstants.LanguageKey, MetadataConstants.LanguageValue);
            dictionary.Add(MetadataConstants.ClientVersionKey, MetadataConstants.Instance.ClientVersion);
            dictionary.Add(MetadataConstants.RequestIdKey, Guid.NewGuid().ToString());
            dictionary.Add(MetadataConstants.ClientIdKey, client.GetClientId());
            dictionary.Add(MetadataConstants.NamespaceKey, client.GetClientConfig().Namespace);

            var time = DateTime.Now.ToString(MetadataConstants.DateTimeFormat);
            dictionary.Add(MetadataConstants.DateTimeKey, time);

            var credentials = clientConfig.SessionCredentialsProvider?.SessionCredentials;
            if (credentials == null)
            {
                return dictionary;
            }

            if (!string.IsNullOrEmpty(credentials.SecurityToken))
            {
                dictionary.Add(MetadataConstants.SessionTokenKey, credentials.SecurityToken);
            }

            var secretData = Encoding.ASCII.GetBytes(credentials.AccessSecret);
            var data = Encoding.ASCII.GetBytes(time);
            using (var signer = new HMACSHA1(secretData))
            {
                var digest = signer.ComputeHash(data);
                var hmac = BitConverter.ToString(digest).Replace("-", "");
                var authorization = $"{MetadataConstants.AlgorithmKey} " +
                                    $"{MetadataConstants.CredentialKey}={credentials.AccessKey}, " +
                                    $"{MetadataConstants.SignedHeadersKey}={MetadataConstants.DateTimeKey}, " +
                                    $"{MetadataConstants.SignatureKey}={hmac}";
                dictionary.Add(MetadataConstants.Authorization, authorization);
                return dictionary;
            }
        }
    }
}