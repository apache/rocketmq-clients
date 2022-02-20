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

namespace org.apache.rocketmq
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
    }
}