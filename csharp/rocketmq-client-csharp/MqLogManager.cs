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
using NLog;
using NLog.Config;
using NLog.Layouts;
using NLog.Targets;
using NLog.Targets.Wrappers;

namespace Org.Apache.Rocketmq
{
    /**
     * RocketMQ Log Manager.
     *
     * Configure component logging, please refer to https://github.com/NLog/NLog/wiki/Configure-component-logging
     */
    public static class MqLogManager
    {
        public static LogFactory Instance => LazyInstance.Value;

        private static readonly Lazy<LogFactory> LazyInstance = new Lazy<LogFactory>(BuildLogFactory);

        private const string FileLogLevelKey = "rocketmq_log_level";
        private const string FileLogLevel = "Info";

        private const string ConsoleAppenderEnabledKey = "mq_consoleAppender_enabled";
        private const string ConsoleAppenderEnabled = "false";
        private const string ConsoleAppenderLogLevel = "Off";


        private const string FileLogRootKey = "rocketmq_log_root";

        private const string FileMaxIndexKey = "rocketmq_log_file_maxIndex";
        private const string FileMaxIndex = "10";

        private static LogFactory BuildLogFactory()
        {
            var fileLogLevel = Environment.GetEnvironmentVariable(FileLogLevelKey) ?? FileLogLevel;
            var consoleAppenderEnabled =
                Environment.GetEnvironmentVariable(ConsoleAppenderEnabledKey) ?? ConsoleAppenderEnabled;
            var consoleLogLevel = bool.Parse(consoleAppenderEnabled) ? fileLogLevel : ConsoleAppenderLogLevel;
            var fileLogRoot = Environment.GetEnvironmentVariable(FileLogRootKey) ??
                              Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            var fileMaxIndexStr = Environment.GetEnvironmentVariable(FileMaxIndexKey) ?? FileMaxIndex;
            var fileMaxIndex = int.Parse(fileMaxIndexStr);


            var config = new LoggingConfiguration();
            var fileTarget = new FileTarget();
            fileTarget.Name = "log_file";
            fileTarget.FileName =
                new SimpleLayout($"{fileLogRoot}/logs/rocketmq/rocketmq-client.log");
            fileTarget.Layout =
                new SimpleLayout(
                    "${longdate} ${level:uppercase=true:padding=-5} [${processid}] [${threadid}] [${callsite}:${callsite-linenumber}] ${message} ${onexception:${exception:format=ToString,Data}}");
            fileTarget.ArchiveFileName =
                new SimpleLayout("${specialfolder:folder=UserProfile}/logs/rocketmq/rocketmq-client.{######}.log");
            fileTarget.ArchiveAboveSize = 67108864;
            fileTarget.ArchiveNumbering = ArchiveNumberingMode.DateAndSequence;
            fileTarget.MaxArchiveFiles = fileMaxIndex;
            fileTarget.ConcurrentWrites = true;
            fileTarget.KeepFileOpen = false;

            var asyncTargetWrapper = new AsyncTargetWrapper(fileTarget);
            asyncTargetWrapper.Name = "asyncFile";
            config.AddTarget(asyncTargetWrapper);

            var consoleTarget = new ColoredConsoleTarget();
            consoleTarget.Name = "colorConsole";
            consoleTarget.UseDefaultRowHighlightingRules = true;
            consoleTarget.Layout =
                new SimpleLayout(
                    "${longdate} ${level:uppercase=true:padding=-5} [${processid}] [${threadid}] [${callsite}:${callsite-linenumber}] ${message} ${onexception:${exception:format=ToString,Data}}");

            config.AddTarget(consoleTarget);

            var asyncFileRule = new LoggingRule("*", LogLevel.FromString(fileLogLevel), asyncTargetWrapper);
            config.LoggingRules.Add(asyncFileRule);

            var consoleRule = new LoggingRule("*", LogLevel.FromString(consoleLogLevel), consoleTarget);
            config.LoggingRules.Add(consoleRule);

            var logFactory = new LogFactory();
            logFactory.Configuration = config;
            return logFactory;
        }
    }
}