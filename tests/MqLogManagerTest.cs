using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NLog;
using org.apache.rocketmq;

namespace tests
{
    [TestClass]
    public class MqLogManagerTest
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        [TestMethod]
        public void TestLog()
        {
            Logger.Trace("This is a trace message.");
            Logger.Debug("This is a debug message.");
            Logger.Info("This is a info message.");
            Logger.Warn("This is a warn message.");
            Logger.Error("This is a error message.");
            Logger.Fatal("This is a fatal message.");

            Logger.Error(new Exception("foobar"), "this is a error message with exception.");
            Logger.Fatal(new Exception("foobar"), "this is a fatal message with exception.");
        }
    }
}