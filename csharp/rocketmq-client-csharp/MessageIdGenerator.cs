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
using System.Diagnostics;
using System.IO;
using System.Threading;

namespace Org.Apache.Rocketmq
{
    /**
     * MessageId generate rules refer: https://yuque.antfin-inc.com/aone709911/ca1edg/af2t6o
     */
    public class MessageIdGenerator
    {
        public static readonly string version = "01";
        private static readonly MessageIdGenerator Instance = new();

        private readonly string _prefix;

        private readonly long _secondsSinceCustomEpoch;
        private readonly Stopwatch _stopwatch;

        private int _sequence;

        private MessageIdGenerator()
        {
            MemoryStream stream = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(stream);

            var macAddress = Utilities.GetMacAddress();
            writer.Write(macAddress, 0, 6);

            int processId = Utilities.GetProcessId();

            byte[] processIdBytes = BitConverter.GetBytes(processId);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(processIdBytes);
            }

            writer.Write(processIdBytes, 2, 2);
            var array = stream.ToArray();
            _prefix = version + Utilities.ByteArrayToHexString(array);

            DateTime epoch = new DateTime(2021, 1, 1,
                0, 0, 0, 0, DateTimeKind.Utc);

            var now = DateTime.Now;
            _secondsSinceCustomEpoch = Convert.ToInt64(now.ToUniversalTime().Subtract(epoch).TotalSeconds);
            _stopwatch = Stopwatch.StartNew();

            _sequence = 0;
        }

        public String Next()
        {
            long deltaSeconds = _secondsSinceCustomEpoch + _stopwatch.ElapsedMilliseconds / 1_000;

            MemoryStream stream = new MemoryStream();
            BinaryWriter writer = new BinaryWriter(stream);

            byte[] deltaSecondsBytes = BitConverter.GetBytes(deltaSeconds);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(deltaSecondsBytes);
            }

            writer.Write(deltaSecondsBytes, 4, 4);

            int no = Interlocked.Increment(ref _sequence);
            byte[] noBytes = BitConverter.GetBytes(no);
            if (BitConverter.IsLittleEndian)
            {
                Array.Reverse(noBytes);
            }

            writer.Write(noBytes);
            var suffixBytes = stream.ToArray();

            return _prefix + Utilities.ByteArrayToHexString(suffixBytes);
        }


        public static MessageIdGenerator GetInstance()
        {
            return Instance;
        }
    }
}