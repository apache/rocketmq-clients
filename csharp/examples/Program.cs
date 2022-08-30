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
using Org.Apache.Rocketmq;

namespace examples
{
    class Program
    {
        private const string accessUrl = "rmq-cn-tl32uly8x0n.cn-hangzhou.rmq.aliyuncs.com:8080";
        private const string standardTopic = "sdk_standard";
        private const string fifoTopic = "sdk_fifo";
        private const string timedTopic = "sdk_timed";
        private const string transactionalTopic = "sdk_transactional";
        
        private static async Task<SendReceipt> SendStandardMessage(Producer producer)
        {
            byte[] body = new byte[1024];
            Array.Fill(body, (byte)'x');
            // Associate the message with one or multiple keys
            var keys = new List<string>
            {
                "k1",
                "k2"
            };
            
            var msg = new Message(standardTopic, body)
            {
                // Tag the massage. A message has at most one tag.
                Tag = "Tag-0",
                Keys = keys
            };
            
            msg.Keys = keys;

            return await producer.Send(msg);
        }

        private static async Task<SendReceipt> SendFifoMessage(Producer producer)
        {
            byte[] body = new byte[1024];
            Array.Fill(body, (byte)'x');
            // Associate the message with one or multiple keys
            var keys = new List<string>
            {
                "k1",
                "k2"
            };
            
            var msg = new Message(fifoTopic, body)
            {
                // Tag the massage. A message has at most one tag.
                Tag = "Tag-0",
                Keys = keys
            };
            
            msg.Keys = keys;

            // Messages of the same message-group will be published orderly.
            msg.MessageGroup = "SampleMessageGroup";

            return await producer.Send(msg);
        }

        private static async Task<SendReceipt> SendTimedMessage(Producer producer)
        {
            byte[] body = new byte[1024];
            Array.Fill(body, (byte)'x');
            // Associate the message with one or multiple keys
            var keys = new List<string>
            {
                "k1",
                "k2"
            };
            
            var msg = new Message(timedTopic, body)
            {
                // Tag the massage. A message has at most one tag.
                Tag = "Tag-0",
                Keys = keys
            };
            
            msg.Keys = keys;
            msg.DeliveryTimestamp = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            return await producer.Send(msg);
        }
        
        static async Task Main(string[] args)
        {
            var credentialsProvider = new ConfigFileCredentialsProvider();
            var producer = new Producer(accessUrl);
            producer.CredentialsProvider = credentialsProvider;
            producer.AddTopicOfInterest(standardTopic);
            producer.AddTopicOfInterest(fifoTopic);
            producer.AddTopicOfInterest(timedTopic);
            producer.AddTopicOfInterest(transactionalTopic);

            await producer.Start();

            var sendReceiptOfStandardMessage = await SendStandardMessage(producer);
            Console.WriteLine($"Standard message-id: {sendReceiptOfStandardMessage.MessageId}");

            var sendReceiptOfFifoMessage = await SendFifoMessage(producer);
            Console.WriteLine($"FIFO message-id: {sendReceiptOfFifoMessage.MessageId}");
            
            var sendReceiptOfTimedMessage = await SendTimedMessage(producer);
            Console.WriteLine($"Timed message-id: {sendReceiptOfTimedMessage.MessageId}");

            Console.ReadKey();
        }
    }
}
