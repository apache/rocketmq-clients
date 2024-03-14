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

using System.IO.Compression;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    [TestClass]
    public class UtilitiesTest
    {
        [TestMethod]
        public void TestDecompressBytesGzip()
        {
            var originalData = new byte[] { 1, 2, 3, 4, 5 };
            var compressedData = Utilities.CompressBytesGzip(originalData, CompressionLevel.Fastest);
            CollectionAssert.AreEqual(Utilities.DecompressBytesGzip(compressedData), originalData);
        }

        [TestMethod]
        public void TestComputeMd5Hash()
        {
            var bytes = Encoding.UTF8.GetBytes("foobar");
            Assert.AreEqual(Utilities.ComputeMd5Hash(bytes), "3858F62230AC3C915F300C664312C63F");
        }

        [TestMethod]
        public void TestComputeSha1Hash()
        {
            var bytes = Encoding.UTF8.GetBytes("foobar");
            Assert.AreEqual(Utilities.ComputeSha1Hash(bytes), "8843D7F92416211DE9EBB963FF4CE28125932878");
        }

        [TestMethod]
        public void TestGetMacAddress()
        {
            var macAddress = Utilities.GetMacAddress();
            Assert.IsTrue(macAddress != null && macAddress.Length >= 6);
        }
    }
}
