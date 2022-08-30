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
        static async Task Main(string[] args)
        {
            string accessUrl = "rmq-cn-tl32uly8x0n.cn-hangzhou.rmq.aliyuncs.com:8080";
            var topic = "sdk_standard";
            var credentialsProvider = new ConfigFileCredentialsProvider();
            var producer = new Producer(accessUrl);
            producer.CredentialsProvider = credentialsProvider;
            producer.AddTopicOfInterest(topic);
            
            await producer.Start();

            byte[] body = new byte[1024];
            Array.Fill(body, (byte)'x');
            // Associate the message with one or multiple keys
            var keys = new List<string>
            {
                "k1",
                "k2"
            };
            
            var msg = new Message(topic, body)
            {
                // Tag the massage. A message has at most one tag.
                Tag = "Tag-0",
                Keys = keys
            };
            
            msg.Keys = keys;

            var sendReceipt = await producer.Send(msg);
            Console.WriteLine(sendReceipt.MessageId);
            
            Console.ReadKey();
        }
    }
}
