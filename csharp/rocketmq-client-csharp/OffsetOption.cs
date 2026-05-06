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

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Represents the offset option for consuming messages from a specific position.
    /// </summary>
    public class OffsetOption
    {
        public const long PolicyLastValue = 0L;
        public const long PolicyMinValue = 1L;
        public const long PolicyMaxValue = 2L;

        public static readonly OffsetOption LastOffset = new OffsetOption(OffsetType.Policy, PolicyLastValue);
        public static readonly OffsetOption MinOffset = new OffsetOption(OffsetType.Policy, PolicyMinValue);
        public static readonly OffsetOption MaxOffset = new OffsetOption(OffsetType.Policy, PolicyMaxValue);

        private readonly OffsetType _type;
        private readonly long _value;

        private OffsetOption(OffsetType type, long value)
        {
            _type = type;
            _value = value;
        }

        /// <summary>
        /// Creates an offset option from a specific offset value.
        /// </summary>
        /// <param name="offset">The offset value (must be >= 0)</param>
        /// <returns>OffsetOption instance</returns>
        public static OffsetOption OfOffset(long offset)
        {
            if (offset < 0)
            {
                throw new ArgumentException("offset must be greater than or equal to 0");
            }
            return new OffsetOption(OffsetType.Offset, offset);
        }

        /// <summary>
        /// Creates an offset option from tail N messages.
        /// </summary>
        /// <param name="tailN">Number of messages from tail (must be >= 0)</param>
        /// <returns>OffsetOption instance</returns>
        public static OffsetOption OfTailN(long tailN)
        {
            if (tailN < 0)
            {
                throw new ArgumentException("tailN must be greater than or equal to 0");
            }
            return new OffsetOption(OffsetType.TailN, tailN);
        }

        /// <summary>
        /// Creates an offset option from a timestamp.
        /// </summary>
        /// <param name="timestamp">Unix timestamp in milliseconds (must be >= 0)</param>
        /// <returns>OffsetOption instance</returns>
        public static OffsetOption OfTimestamp(long timestamp)
        {
            if (timestamp < 0)
            {
                throw new ArgumentException("timestamp must be greater than or equal to 0");
            }
            return new OffsetOption(OffsetType.Timestamp, timestamp);
        }

        public OffsetType Type => _type;
        public long Value => _value;

        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            var other = (OffsetOption)obj;
            return _value == other._value && _type == other._type;
        }

        public override int GetHashCode()
        {
            unchecked
            {
                int hashCode = (int)_type;
                hashCode = (hashCode * 397) ^ _value.GetHashCode();
                return hashCode;
            }
        }

        public override string ToString()
        {
            return $"OffsetOption(type={_type}, value={_value})";
        }
    }

    /// <summary>
    /// Type of offset option.
    /// </summary>
    public enum OffsetType
    {
        Policy,
        Offset,
        TailN,
        Timestamp
    }
}
