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
using System.Buffers.Binary;
using System.Threading;

namespace Org.Apache.Rocketmq
{
    /**
     * MessageId generate rules refer: https://yuque.antfin-inc.com/aone709911/ca1edg/af2t6o
     */
    public class MessageIdGenerator
    {
        private readonly TimeProvider _timeProvider;
        private readonly IUtilities _utilities;
        public const string Version = "01";
        private static readonly MessageIdGenerator Instance = new MessageIdGenerator(TimeProvider.System, DefaultUtilities.Instance);

        private readonly string _prefix;

        private readonly long _secondsSinceCustomEpoch;
        private readonly long _startTimestamp;

        private int _sequence;

        internal MessageIdGenerator(TimeProvider timeProvider, IUtilities utilities)
        {
            _timeProvider = timeProvider;
            _utilities = utilities;

            Span<byte> buffer = stackalloc byte[8]; // 6 bytes for MAC + 2 bytes for ProcessID
            utilities.GetMacAddress().AsSpan().CopyTo(buffer);

            var processId = utilities.GetProcessId();
            BinaryPrimitives.WriteInt16BigEndian(buffer.Slice(6, 2), (short)processId);

            _prefix = Version + utilities.ByteArrayToHexString(buffer);

            var epoch = new DateTime(2021, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc);
            var now = timeProvider.GetUtcNow();
            _secondsSinceCustomEpoch = Convert.ToInt64(now.Subtract(epoch).TotalSeconds);
            _startTimestamp = timeProvider.GetTimestamp();

            _sequence = 0;
        }

        public string Next()
        {
            var deltaSeconds = _secondsSinceCustomEpoch + (long)_timeProvider.GetElapsedTime(_startTimestamp).TotalSeconds;

            Span<byte> buffer = stackalloc byte[8];

            BinaryPrimitives.WriteInt32BigEndian(buffer[..4], (int)deltaSeconds);

            var no = Interlocked.Increment(ref _sequence);
            BinaryPrimitives.WriteInt32BigEndian(buffer[4..4], no);

            return $"{_prefix}{_utilities.ByteArrayToHexString(buffer)}";
        }

        public static MessageIdGenerator GetInstance()
        {
            return Instance;
        }
    }
}