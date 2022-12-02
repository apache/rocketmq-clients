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
using System.Text;
using System.Threading.Tasks;
using NLog;
using Org.Apache.Rocketmq;

namespace examples
{
    static class ProducerFifoMessageExample
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        internal static async Task QuickStart()
        {
            string accessKey = "yourAccessKey";
            string secretKey = "yourSecretKey";
            // Credential provider is optional for client configuration.
            var credentialsProvider = new StaticCredentialsProvider(accessKey, secretKey);
            string endpoints = "foobar.com:8080";

            var producer = new Producer(endpoints)
            {
                CredentialsProvider = credentialsProvider
            };
            string topic = "yourFifoTopic";
            // Set the topic name(s), which is optional. It makes producer could prefetch the topic route before 
            // message publishing.
            producer.AddTopicOfInterest(topic);

            await producer.Start();
            // Define your message body.
            byte[] bytes = Encoding.UTF8.GetBytes("foobar");
            string tag = "yourMessageTagA";
            // You could set multiple keys for the single message.
            var keys = new List<string>
            {
                "yourMessageKey-6cc8b65ed1c8",
                "yourMessageKey-43783375d9a5"
            };
            // Set topic for current message.
            var message = new Message(topic, bytes)
            {
                Tag = tag,
                Keys = keys,
                // Essential for FIFO message, messages that belongs to the same message group follow the FIFO semantics.
                MessageGroup = "yourMessageGroup0"
            };
            var sendReceipt = await producer.Send(message);
            Logger.Info($"Send message successfully, sendReceipt={sendReceipt}.");
            await producer.Shutdown();
        }
    }
}