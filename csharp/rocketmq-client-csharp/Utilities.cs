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

using System.Diagnostics;
using System.Linq;
using System.Net.NetworkInformation;
using System.Text;
using System;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    public static class Utilities
    {
        public static byte[] GetMacAddress()
        {
            return NetworkInterface.GetAllNetworkInterfaces().FirstOrDefault(nic =>
                nic.OperationalStatus == OperationalStatus.Up &&
                nic.NetworkInterfaceType != NetworkInterfaceType.Loopback)?.GetPhysicalAddress().GetAddressBytes();
        }

        public static int GetProcessId()
        {
            return Process.GetCurrentProcess().Id;
        }

        public static string ByteArrayToHexString(byte[] bytes)
        {
            StringBuilder result = new StringBuilder(bytes.Length * 2);
            const string hexAlphabet = "0123456789ABCDEF";

            foreach (byte b in bytes)
            {
                result.Append(hexAlphabet[(int)(b >> 4)]);
                result.Append(hexAlphabet[(int)(b & 0xF)]);
            }

            return result.ToString();
        }

        public static string TargetUrl(rmq::MessageQueue messageQueue)
        {
            // TODO: Assert associated broker has as least one service endpoint.
            var serviceEndpoint = messageQueue.Broker.Endpoints.Addresses[0];
            return $"https://{serviceEndpoint.Host}:{serviceEndpoint.Port}";
        }

        public static int CompareMessageQueue(rmq::MessageQueue lhs, rmq::MessageQueue rhs)
        {
            int topic_comparison = String.Compare(lhs.Topic.ResourceNamespace + lhs.Topic.Name, rhs.Topic.ResourceNamespace + rhs.Topic.Name);
            if (topic_comparison != 0)
            {
                return topic_comparison;
            }

            int broker_name_comparison = String.Compare(lhs.Broker.Name, rhs.Broker.Name);
            if (0 != broker_name_comparison)
            {
                return broker_name_comparison;
            }

            return lhs.Id < rhs.Id ? -1 : (lhs.Id == rhs.Id ? 0 : 1);
        }
    }
}