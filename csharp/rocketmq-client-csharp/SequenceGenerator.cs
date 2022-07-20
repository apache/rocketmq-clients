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
using System.Threading;
using System.Net.NetworkInformation;
using NLog;

namespace Org.Apache.Rocketmq
{
    /**
     * See https://yuque.antfin-inc.com/aone709911/ca1edg/af2t6o for Sequence ID spec.
     * 
     * In the implementation layer, this class follows Singleton pattern.
     */
    public sealed class SequenceGenerator
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        public static SequenceGenerator Instance
        {
            get
            {
                return Nested.instance;
            }
        }

        private class Nested
        {
            static Nested()
            {

            }

            internal static readonly SequenceGenerator instance = new SequenceGenerator();
        }

        private SequenceGenerator()
        {
            currentSecond = SecondsSinceCustomEpoch();
            macAddress = MacAddress();
            pidBytes = ToArray(pid);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(version);
            }
        }

        /**
         * Sequence version, 2 bytes.
         */
        private static byte[] version = new byte[2] { 0x00, 0x01 };

        /**
         * MAC address, 6 bytes.
         */
        private byte[] macAddress;

        private int sequenceInSecond = 0;
        private int currentSecond;

        private static int pid = System.Diagnostics.Process.GetCurrentProcess().Id;
        private static byte[] pidBytes;

        private static byte[] ToArray(int number)
        {
            byte[] bytes = BitConverter.GetBytes(number);
            if (BitConverter.IsLittleEndian)
                Array.Reverse(bytes);
            return bytes;
        }

        private static int SecondsSinceCustomEpoch()
        {
            var customEpoch = new DateTime(2021, 01, 01, 00, 00, 00, DateTimeKind.Utc);
            var diff = DateTime.UtcNow.Subtract(customEpoch);
            return (int)diff.TotalSeconds;
        }

        private static byte[] MacAddress()
        {
            foreach (var nic in NetworkInterface.GetAllNetworkInterfaces())
            {
                if (nic.OperationalStatus == OperationalStatus.Up)
                {
                    if (nic.Name.StartsWith("lo"))
                    {
                        continue;
                    }
                    Logger.Debug($"NIC={nic.Name}");
                    return nic.GetPhysicalAddress().GetAddressBytes();
                }
            }
            return null;
        }

        public string Next()
        {
            byte[] data = new byte[18];
            Array.Copy(version, 0, data, 0, 2);
            Array.Copy(macAddress, 0, data, 2, 6);
            Array.Copy(pidBytes, 2, data, 8, 2);
            int second = SecondsSinceCustomEpoch();
            if (second != currentSecond)
            {
                currentSecond = second;
                Interlocked.Exchange(ref sequenceInSecond, 0);
            }
            byte[] secondBytes = ToArray(second);
            Array.Copy(secondBytes, 0, data, 10, 4);
            int sequence = Interlocked.Increment(ref sequenceInSecond);
            byte[] sequenceBytes = ToArray(sequence);
            Array.Copy(sequenceBytes, 0, data, 14, 4);
            return BitConverter.ToString(data).Replace("-", ""); ;
        }
    }

}