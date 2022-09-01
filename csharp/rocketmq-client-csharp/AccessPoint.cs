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
using rmq = Apache.Rocketmq.V2;
using System.Net;
using System.Net.Sockets;

namespace Org.Apache.Rocketmq
{
    public class AccessPoint
    {
        public AccessPoint()
        {
            
        }

        public AccessPoint(string accessUrl)
        {
            string[] segments = accessUrl.Split(":");
            if (segments.Length != 2)
            {
                throw new ArgumentException("Access url should be of format host:port");
            }

            Host = segments[0];
            Port = Int32.Parse(segments[1]);
        }

        public string Host { get; }

        public int Port { get; set; }

        public string TargetUrl()
        {
            return $"https://{Host}:{Port}";
        }

        public rmq::AddressScheme HostScheme()
        {
            return SchemeOf(Host);
        }

        private static rmq::AddressScheme SchemeOf(string host)
        {
            var result = IPAddress.TryParse(host, out var ip);
            if (!result)
            {
                return rmq::AddressScheme.DomainName;
            }

            return ip.AddressFamily switch
            {
                AddressFamily.InterNetwork => rmq::AddressScheme.Ipv4,
                AddressFamily.InterNetworkV6 => rmq::AddressScheme.Ipv6,
                _ => rmq::AddressScheme.Unspecified
            };
        }
    }
}
