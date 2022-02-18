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
using System.Threading.Tasks;
using rmq = apache.rocketmq.v1;
using pb = global::Google.Protobuf;
using grpc = global::Grpc.Core;
using System.Collections.Generic;
using System.Collections.Concurrent;


namespace org.apache.rocketmq
{
    public class Producer : Client, IProducer
    {
        public Producer(INameServerResolver resolver) : base(resolver)
        {
            this.loadBalancer = new ConcurrentDictionary<string, PublishLoadBalancer>();
        }

        public override void start()
        {
            base.start();
            // More initalization
        }

        public override void shutdown()
        {
            // Release local resources
            base.shutdown();
        }

        public override void prepareHeartbeatData(rmq::HeartbeatRequest request)
        {

        }

        public async Task<SendResult> send(Message message)
        {
            if (!loadBalancer.ContainsKey(message.Topic))
            {
                var topicRouteData = await getRouteFor(message.Topic, false);
                if (null == topicRouteData || null == topicRouteData.Partitions || 0 == topicRouteData.Partitions.Count)
                {
                    throw new TopicRouteException(string.Format("No topic route for {0}", message.Topic));
                }

                var loadBalancerItem = new PublishLoadBalancer(topicRouteData);
                loadBalancer.TryAdd(message.Topic, loadBalancerItem);
            }

            var publishLB = loadBalancer[message.Topic];

            var request = new rmq::SendMessageRequest();
            request.Message = new rmq::Message();
            request.Message.Body = pb::ByteString.CopyFrom(message.Body);
            request.Message.Topic = new rmq::Resource();
            request.Message.Topic.ResourceNamespace = resourceNamespace();
            request.Message.Topic.Name = message.Topic;

            // User properties
            foreach (var item in message.UserProperties)
            {
                request.Message.UserAttribute.Add(item.Key, item.Value);
            }

            request.Message.SystemAttribute = new rmq::SystemAttribute();
            if (!string.IsNullOrEmpty(message.Tag))
            {
                request.Message.SystemAttribute.Tag = message.Tag;
            }

            if (0 != message.Keys.Count)
            {
                foreach (var key in message.Keys)
                {
                    request.Message.SystemAttribute.Keys.Add(key);
                }
            }

            // string target = "https://";
            List<string> targets = new List<string>();
            List<Partition> candidates = publishLB.select(message.MaxAttemptTimes);
            foreach (var partition in candidates)
            {
                targets.Add(partition.Broker.targetUrl());
            }

            var metadata = new grpc::Metadata();
            Signature.sign(this, metadata);

            Exception ex = null;

            foreach (var target in targets)
            {
                try
                {
                    rmq::SendMessageResponse response = await clientManager.sendMessage(target, metadata, request, getIoTimeout());
                    if (null != response && (int)global::Google.Rpc.Code.Ok == response.Common.Status.Code)
                    {
                        var messageId = response.MessageId;
                        return new SendResult(messageId);
                    }
                }
                catch (Exception e)
                {
                    ex = e;
                }
            }

            if (null != ex)
            {
                throw ex;
            }

            throw new Exception("Send message failed");
        }

        private ConcurrentDictionary<string, PublishLoadBalancer> loadBalancer;
    }
}