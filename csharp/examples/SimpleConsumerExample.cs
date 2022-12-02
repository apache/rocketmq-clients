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
using NLog;
using Org.Apache.Rocketmq;

namespace examples
{
    static class SimpleConsumerExample
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        internal static async Task QuickStart()
        {
            string accessKey = "yourAccessKey";
            string secretKey = "yourSecretKey";
            // Credential provider is optional for client configuration.
            var credentialsProvider = new StaticCredentialsProvider(accessKey, secretKey);
            string endpoints = "foobar.com:8080";

            string consumerGroup = "yourConsumerGroup";
            SimpleConsumer simpleConsumer = new SimpleConsumer(endpoints, consumerGroup)
            {
                CredentialsProvider = credentialsProvider
            };

            string topic = "yourTopic";
            string tag = "tagA";
            // Set topic subscription for consumer.
            simpleConsumer.Subscribe(topic, new FilterExpression(tag, ExpressionType.TAG));
            await simpleConsumer.Start();

            int maxMessageNum = 16;
            TimeSpan invisibleDuration = TimeSpan.FromSeconds(15);
            var messages = await simpleConsumer.Receive(maxMessageNum, invisibleDuration);
            Logger.Info($"{messages.Count} messages has been received.");

            var tasks = new List<Task>();
            foreach (var message in messages)
            {
                Logger.Info($"Received a message, topic={message.Topic}, message-id={message.MessageId}.");
                var task = simpleConsumer.Ack(message);
                tasks.Add(task);
            }

            await Task.WhenAll(tasks);
            Logger.Info($"{tasks.Count} messages have been acknowledged.");
        }
    }
}