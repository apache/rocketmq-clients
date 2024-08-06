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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;
using Proto = Apache.Rocketmq.V2;

namespace tests
{
    [TestClass]
    public class PublishingLoadBalancerTest
    {
        [TestMethod]
        public void TestTakeMessageQueues()
        {
            const string host0 = "127.0.0.1";
            const string host1 = "127.0.0.2";
            var mqs = new List<Proto.MessageQueue>();
            var mq0 = new Proto.MessageQueue
            {
                Broker = new Proto.Broker
                {
                    Name = "broker0",
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses =
                        {
                            new Proto.Address
                            {
                                Host = host0,
                                Port = 80
                            }
                        }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "foo-bar-namespace",
                    Name = "TestTopic",
                }
            };
            var mq1 = new Proto.MessageQueue
            {
                Broker = new Proto.Broker
                {
                    Name = "broker1",
                    Endpoints = new Proto.Endpoints
                    {
                        Scheme = Proto.AddressScheme.Ipv4,
                        Addresses =
                        {
                            new Proto.Address
                            {
                                Host = host1,
                                Port = 80
                            }
                        }
                    }
                },
                Id = 0,
                Permission = Proto.Permission.ReadWrite,
                Topic = new Proto.Resource
                {
                    ResourceNamespace = "foo-bar-namespace",
                    Name = "TestTopic",
                }
            };
            mqs.Add(mq0);
            mqs.Add(mq1);
            var topicRouteData = new TopicRouteData(mqs);
            var publishingLoadBalancer = new PublishingLoadBalancer(topicRouteData);
            var endpoints0 = new Endpoints(host0);
            var excluded0 = new HashSet<Endpoints> { endpoints0 };
            var candidates0
                = publishingLoadBalancer.TakeMessageQueues(excluded0, 1);
            Assert.AreEqual(candidates0.Count, 1);
            Assert.AreEqual(host1, candidates0[0].Broker.Endpoints.Addresses[0].Host);
            var endpoints1 = new Endpoints(host1);
            var excluded1 = new HashSet<Endpoints> { endpoints0, endpoints1 };
            var candidates1 = publishingLoadBalancer.TakeMessageQueues(excluded1, 2);
            Assert.AreEqual(2, candidates1.Count);
        }
    }
}