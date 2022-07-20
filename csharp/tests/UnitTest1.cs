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
        public void TestRpcClientImplCtor()
        {
            RpcClient impl = new RpcClient("https://localhost:5001");
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
