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

namespace tests
{
    [TestClass]
    public class ResourceTests
    {
        [TestMethod]
        public void TestGetterAndSetter()
        {
            var resource = new Resource("foobar");
            Assert.AreEqual("foobar", resource.Name);
            Assert.AreEqual(string.Empty, resource.Namespace);

            resource = new Resource("foo", "bar");
            Assert.AreEqual("bar", resource.Name);
            Assert.AreEqual("foo", resource.Namespace);
        }

        [TestMethod]
        public void TestToProtobuf()
        {
            var resource = new Resource("foo", "bar");
            var protobuf = resource.ToProtobuf();
            Assert.AreEqual("foo", protobuf.ResourceNamespace);
            Assert.AreEqual("bar", protobuf.Name);
        }

        [TestMethod]
        public void TestEqual()
        {
            var resource0 = new Resource("foo", "bar");
            var resource1 = new Resource("foo", "bar");
            Assert.AreEqual(resource0, resource1);

            var resource2 = new Resource("foo0", "bar");
            Assert.AreNotEqual(resource0, resource2);
        }
    }
}