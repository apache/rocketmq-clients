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
using grpc = global::Grpc.Core;
using System.Security.Cryptography;

namespace Org.Apache.Rocketmq
{
    public class Signature
    {
        public static void sign(IClientConfig clientConfig, grpc::Metadata metadata)
        {
            metadata.Add(MetadataConstants.LANGUAGE_KEY, "DOTNET");
            metadata.Add(MetadataConstants.CLIENT_VERSION_KEY, "5.0.0");
            metadata.Add(MetadataConstants.CLIENT_ID_KEY, clientConfig.clientId());
            if (!String.IsNullOrEmpty(clientConfig.tenantId()))
            {
                metadata.Add(MetadataConstants.TENANT_ID_KEY, clientConfig.tenantId());
            }

            if (!String.IsNullOrEmpty(clientConfig.resourceNamespace()))
            {
                metadata.Add(MetadataConstants.NAMESPACE_KEY, clientConfig.resourceNamespace());
            }

            string time = DateTime.Now.ToString(MetadataConstants.DATE_TIME_FORMAT);
            metadata.Add(MetadataConstants.DATE_TIME_KEY, time);

            if (null != clientConfig.credentialsProvider())
            {
                var credentials = clientConfig.credentialsProvider().getCredentials();
                if (null == credentials || credentials.expired())
                {
                    return;
                }

                if (!String.IsNullOrEmpty(credentials.SessionToken))
                {
                    metadata.Add(MetadataConstants.STS_SESSION_TOKEN, credentials.SessionToken);
                }

                byte[] secretData = Encoding.ASCII.GetBytes(credentials.AccessSecret);
                byte[] data = Encoding.ASCII.GetBytes(time);
                HMACSHA1 signer = new HMACSHA1(secretData);
                byte[] digest = signer.ComputeHash(data);
                string hmac = BitConverter.ToString(digest).Replace("-", "");
                string authorization = string.Format("{0} {1}={2}/{3}/{4}, {5}={6}, {7}={8}",
                    MetadataConstants.ALGORITHM_KEY,
                    MetadataConstants.CREDENTIAL_KEY,
                    credentials.AccessKey,
                    clientConfig.region(),
                    clientConfig.serviceName(),
                    MetadataConstants.SIGNED_HEADERS_KEY,
                    MetadataConstants.DATE_TIME_KEY,
                    MetadataConstants.SIGNATURE_KEY,
                    hmac);
                metadata.Add(MetadataConstants.AUTHORIZATION, authorization);
            }
        }
    }
}