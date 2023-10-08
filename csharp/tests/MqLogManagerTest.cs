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

using System;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Org.Apache.Rocketmq;

namespace tests
{
    [TestClass]
    public class MqLogManagerTest
    {
        private static readonly ILogger DefaultLogger1;
        private static readonly ILogger DefaultLogger2;
        private static readonly ILogger ConsoleLogger1;
        private static readonly ILogger ConsoleLogger2;

        static MqLogManagerTest()
        {
            DefaultLogger1 = MqLogManager.CreateLogger<MqLogManagerTest>();
            DefaultLogger2 = MqLogManager.CreateLogger("MqLogManagerTest2");

            var loggerFactory = LoggerFactory.Create(
                builder => builder
                    .AddFilter("tests", LogLevel.Information)
                    .AddConsole());
            MqLogManager.UseLoggerFactory(loggerFactory);
            ConsoleLogger1 = MqLogManager.CreateLogger<MqLogManagerTest>();
            ConsoleLogger2 = MqLogManager.CreateLogger("MqLogManagerTest2");
        }

        [TestMethod]
        public void TestLog()
        {
            TestLog(DefaultLogger1);
            TestLog(DefaultLogger2);
            TestLog(ConsoleLogger1);
            TestLog(ConsoleLogger2);
        }

        private void TestLog(ILogger logger)
        {
            logger.LogTrace("This is a trace message.");
            logger.LogDebug("This is a debug message.");
            logger.LogInformation("This is an info message.");
            logger.LogWarning("This is a warn message.");
            logger.LogError("This is an error message.");
            logger.LogCritical("This is a critical message.");

            logger.LogError(new Exception("foobar"), "this is an error message with exception.");
            logger.LogCritical(new Exception("foobar"), "this is a critical message with exception.");
        }
    }
}