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
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Collections.Generic;

namespace Org.Apache.Rocketmq
{

    [TestClass]
    public class TopicTest
    {

        [TestMethod]
        public void testCompareTo()
        {
            List<Topic> topics = new List<Topic>();
            topics.Add(new Topic("ns1", "t1"));
            topics.Add(new Topic("ns0", "t1"));
            topics.Add(new Topic("ns0", "t0"));

            topics.Sort();

            Assert.AreEqual(topics[0].ResourceNamespace, "ns0");
            Assert.AreEqual(topics[0].Name, "t0");

            Assert.AreEqual(topics[1].ResourceNamespace, "ns0");
            Assert.AreEqual(topics[1].Name, "t1");


            Assert.AreEqual(topics[2].ResourceNamespace, "ns1");
            Assert.AreEqual(topics[2].Name, "t1");

        }


    }
}