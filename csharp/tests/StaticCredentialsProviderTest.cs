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


namespace Org.Apache.Rocketmq
{
    [TestClass]
    public class StaticCredentialsProviderTest
    {

        [TestMethod]
        public void testGetCredentials()
        {
            var accessKey = "key";
            var accessSecret = "secret";
            var provider = new StaticCredentialsProvider(accessKey, accessSecret);
            var credentials = provider.getCredentials();
            Assert.IsNotNull(credentials);
            Assert.IsFalse(credentials.expired(), "Credentials from StaticCredentialsProvider should never expire");
            Assert.AreEqual(credentials.AccessKey, accessKey);
            Assert.AreEqual(credentials.AccessSecret, accessSecret);
        }

    }
}