using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    [TestClass]
    public class EndpointsTest
    {
        [TestMethod]
        public void testConstructor()
        {
            Console.WriteLine(Uri.CheckHostName("127.0.0.1"));
            Console.WriteLine(Uri.CheckHostName("1050:0000:0000:0000:0005:0600:300c:326b"));
            Console.WriteLine(Uri.CheckHostName("baidu.com"));
        }
    }
}