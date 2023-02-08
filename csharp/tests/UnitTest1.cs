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
using Org.Apache.Rocketmq;
using Grpc.Net.Client;
using rmq = Apache.Rocketmq.V2;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            rmq::Permission perm = rmq::Permission.None;
            switch (perm)
            {
                case rmq::Permission.None:
                    {
                        Console.WriteLine("None");
                        break;
                    }

                case rmq::Permission.Read:
                    {
                        Console.WriteLine("Read");
                        break;
                    }

                case rmq::Permission.Write:
                    {
                        Console.WriteLine("Write");
                        break;
                    }

                case rmq::Permission.ReadWrite:
                    {
                        Console.WriteLine("ReadWrite");
                        break;
                    }

            }
        }

        [TestMethod]
        public void TestConcurrentDictionary()
        {
            var dict = new ConcurrentDictionary<string, List<String>>();
            string s = "abc";
            List<String> result;
            var exists = dict.TryGetValue(s, out result);
            Assert.IsFalse(exists);
            Assert.IsNull(result);

            result = new List<string>();
            result.Add("abc");
            Assert.IsTrue(dict.TryAdd(s, result));

            List<String> list;
            exists = dict.TryGetValue(s, out list);
            Assert.IsTrue(exists);
            Assert.IsNotNull(list);
            Assert.AreEqual(1, list.Count);
        }
    }
}
