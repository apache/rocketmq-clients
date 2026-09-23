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

using System.Collections.Generic;
using System.Threading.Tasks;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Lite simple consumer, a simple consumer that is bound to one parent topic and
    /// consumes messages from the lite topics it subscribes to.
    /// </summary>
    public interface ILiteSimpleConsumer
    {
        /// <summary>
        /// Subscribe to a lite topic.
        /// </summary>
        /// <param name="liteTopic">The name of the lite topic to subscribe</param>
        Task SubscribeLite(string liteTopic);

        /// <summary>
        /// Subscribe to a lite topic with an offset option to specify the consume from offset.
        /// </summary>
        /// <param name="liteTopic">The name of the lite topic to subscribe</param>
        /// <param name="offsetOption">The consume from offset option</param>
        Task SubscribeLite(string liteTopic, OffsetOption offsetOption);

        /// <summary>
        /// Unsubscribe from a lite topic.
        /// </summary>
        /// <param name="liteTopic">The name of the lite topic to unsubscribe from</param>
        Task UnsubscribeLite(string liteTopic);

        /// <summary>
        /// Get the lite topic immutable set.
        /// </summary>
        /// <returns>Lite topic immutable set</returns>
        ISet<string> GetLiteTopicSet();

        /// <summary>
        /// Get the parent topic this consumer binds to.
        /// </summary>
        /// <returns>The bound parent topic</returns>
        string GetBindTopic();
    }
}
