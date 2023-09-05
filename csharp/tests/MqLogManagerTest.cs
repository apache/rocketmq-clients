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
        private static readonly ILogger Logger1;
        private static readonly ILogger Logger2;
        
        static MqLogManagerTest()
        {
            var loggerFactory = LoggerFactory.Create(
                builder => builder
                    .AddFilter("tests", LogLevel.Information)
                    .AddConsole());
            MqLogManager.UseLoggerFactory(loggerFactory);
            Logger1 = MqLogManager.CreateLogger<MqLogManagerTest>();
            Logger2 = MqLogManager.CreateLogger("MqLogManagerTest2");
        }

        [TestMethod]
        public void TestLog()
        {
            Logger1.LogTrace("This is a trace message.");
            Logger1.LogDebug("This is a debug message.");
            Logger1.LogInformation("This is an info message.");
            Logger1.LogWarning("This is a warn message.");
            Logger1.LogError("This is an error message.");
            Logger1.LogCritical("This is a critical message.");

            Logger1.LogError(new Exception("foobar"), "this is an error message with exception.");
            Logger1.LogCritical(new Exception("foobar"), "this is a critical message with exception.");
            
            Logger2.LogTrace("This is a trace message.");
            Logger2.LogDebug("This is a debug message.");
            Logger2.LogInformation("This is an info message.");
            Logger2.LogWarning("This is a warn message.");
            Logger2.LogError("This is an error message.");
            Logger2.LogCritical("This is a critical message.");
            
            Logger2.LogError(new Exception("foobar"), "this is an error message with exception.");
            Logger2.LogCritical(new Exception("foobar"), "this is a critical message with exception.");
        }
    }
}