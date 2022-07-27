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
using System.IO;
using System.Reflection;
using NLog;
using NLog.Config;

namespace Org.Apache.Rocketmq
{
    /**
     * RocketMQ Log Manager.
     *
     * Configure component logging, please refer to https://github.com/NLog/NLog/wiki/Configure-component-logging
     */
    public class MqLogManager
    {
        public static LogFactory Instance
        {
            get { return LazyInstance.Value; }
        }

        private static readonly Lazy<LogFactory> LazyInstance = new(BuildLogFactory);

        private static LogFactory BuildLogFactory()
        {
            // Use name of current assembly to construct NLog config filename 
            Assembly thisAssembly = Assembly.GetExecutingAssembly();
            string configFilePath = Path.ChangeExtension(thisAssembly.Location, ".nlog");

            LogFactory logFactory = new LogFactory();
            logFactory.Configuration = new XmlLoggingConfiguration(configFilePath, logFactory);
            return logFactory;
        }
    }
}