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
using Proto = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Helper class for converting OffsetOption to Protobuf format.
    /// </summary>
    internal static class OffsetOptionHelper
    {
        public static Proto.OffsetOption ToProtobuf(OffsetOption offsetOption)
        {
            if (offsetOption == null)
            {
                return null;
            }

            var protoBuilder = new Proto.OffsetOption();

            switch (offsetOption.Type)
            {
                case OffsetType.Policy:
                    protoBuilder.Policy = ToProtobufPolicy(offsetOption.Value);
                    break;
                case OffsetType.Offset:
                    protoBuilder.Offset = offsetOption.Value;
                    break;
                case OffsetType.TailN:
                    protoBuilder.TailN = offsetOption.Value;
                    break;
                case OffsetType.Timestamp:
                    protoBuilder.Timestamp = offsetOption.Value;
                    break;
                default:
                    throw new ArgumentException($"Unknown OffsetOption type: {offsetOption.Type}");
            }

            return protoBuilder;
        }

        private static Proto.OffsetOption.Types.Policy ToProtobufPolicy(long policyValue)
        {
            if (policyValue == OffsetOption.PolicyLastValue)
            {
                return Proto.OffsetOption.Types.Policy.Last;
            }
            else if (policyValue == OffsetOption.PolicyMinValue)
            {
                return Proto.OffsetOption.Types.Policy.Min;
            }
            else if (policyValue == OffsetOption.PolicyMaxValue)
            {
                return Proto.OffsetOption.Types.Policy.Max;
            }
            else
            {
                throw new ArgumentException($"Unknown policy value: {policyValue}");
            }
        }
    }
}
