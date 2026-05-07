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
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// Lite push consumer interface for consuming messages from lite topics.
    /// Lite topics allow dynamic topic routing without pre-defining all topics.
    /// </summary>
    public interface ILitePushConsumer : IAsyncDisposable, IDisposable
    {
        /// <summary>
        /// Subscribe to a lite topic.
        /// The subscribeLite() method initiates network requests and performs quota verification, so it may fail.
        /// It's important to check the result of this call to ensure that the subscription was successfully added.
        /// Possible failure scenarios include:
        /// 1. Network request errors, which can be retried.
        /// 2. Quota verification failures, indicated by LiteSubscriptionQuotaExceededException. In this case,
        ///    evaluate whether the quota is insufficient and promptly unsubscribe from unused subscriptions
        ///    using UnsubscribeLite() to free up resources.
        /// </summary>
        /// <param name="liteTopic">The name of the lite topic to subscribe</param>
        Task SubscribeLite(string liteTopic);

        /// <summary>
        /// Subscribe to a lite topic with offsetOption to specify the consume from offset.
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
        /// Get the load balancing group for the consumer.
        /// </summary>
        /// <returns>Consumer load balancing group</returns>
        string GetConsumerGroup();
    }
}
