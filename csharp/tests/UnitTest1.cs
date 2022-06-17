using Microsoft.VisualStudio.TestTools.UnitTesting;
using org.apache.rocketmq;
using Grpc.Net.Client;
using apache.rocketmq.v1;
using System;
namespace tests
{
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            apache.rocketmq.v1.Permission perm = apache.rocketmq.v1.Permission.None;
            switch(perm) {
                case apache.rocketmq.v1.Permission.None:
                {
                    Console.WriteLine("None");
                    break;
                }

                case apache.rocketmq.v1.Permission.Read:
                {
                    Console.WriteLine("Read");
                    break;
                }

                case apache.rocketmq.v1.Permission.Write:
                {
                    Console.WriteLine("Write");
                    break;
                }

                case apache.rocketmq.v1.Permission.ReadWrite:
                {
                    Console.WriteLine("ReadWrite");
                    break;
                }

            }
        }

        [TestMethod]
        public void TestRpcClientImplCtor() {
            using var channel = GrpcChannel.ForAddress("https://localhost:5001");
            var client = new MessagingService.MessagingServiceClient(channel);
            RpcClient impl = new RpcClient(client);
        }
    }
}
