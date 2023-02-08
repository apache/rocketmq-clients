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
        public static void Sign(IClientConfig clientConfig, grpc::Metadata metadata)
        {
            metadata.Add(MetadataConstants.LanguageKey, MetadataConstants.LanguageValue);
            metadata.Add(MetadataConstants.ClientVersionKey, MetadataConstants.Instance.ClientVersion);
            metadata.Add(MetadataConstants.ClientIdKey, clientConfig.ClientId);
            
            string time = DateTime.Now.ToString(MetadataConstants.DateTimeFormat);
            metadata.Add(MetadataConstants.DateTimeKey, time);

            if (null != clientConfig.CredentialsProvider)
            {
                var credentials = clientConfig.CredentialsProvider.Credentials;
                if (null == credentials || credentials.expired())
                {
                    return;
                }

                if (!String.IsNullOrEmpty(credentials.SessionToken))
                {
                    metadata.Add(MetadataConstants.SessionTokenKey, credentials.SessionToken);
                }

                byte[] secretData = Encoding.ASCII.GetBytes(credentials.AccessSecret);
                byte[] data = Encoding.ASCII.GetBytes(time);
                HMACSHA1 signer = new HMACSHA1(secretData);
                byte[] digest = signer.ComputeHash(data);
                string hmac = BitConverter.ToString(digest).Replace("-", "");
                string authorization = string.Format("{0} {1}={2}, {3}={4}, {5}={6}",
                    MetadataConstants.AlgorithmKey,
                    MetadataConstants.CredentialKey,
                    credentials.AccessKey,
                    MetadataConstants.SignedHeadersKey,
                    MetadataConstants.DateTimeKey,
                    MetadataConstants.SignatureKey,
                    hmac);
                metadata.Add(MetadataConstants.Authorization, authorization);
            }
        }
    }
}