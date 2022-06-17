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

namespace org.apache.rocketmq {
    [TestClass]
    public class BrokerTest {

        [TestMethod]
        public void testCompareTo() {
            var b1 = new Broker("b1", 0, null);
            var b2 = new Broker("b1", 1, null);
            Assert.AreEqual(b1.CompareTo(b2), -1);
        }

        [TestMethod]
        public void testEquals() {
            var b1 = new Broker("b1", 0, null);
            var b2 = new Broker("b1", 0, null);
            Assert.AreEqual(b1, b2, "Equals method should be employed to test equality");
        }

    }
}