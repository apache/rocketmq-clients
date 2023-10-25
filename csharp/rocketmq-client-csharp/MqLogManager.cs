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
using NLog.Config;
using NLog.Extensions.Logging;
using NLog.Layouts;
using NLog.Targets;
using NLog.Targets.Wrappers;
using LogLevel = NLog.LogLevel;

namespace Org.Apache.Rocketmq
{
    /// <summary>
    /// RocketMQ Log Manager.
    /// Use NLog as the default logger and support custom logger factory by using <see cref="UseLoggerFactory"/>.
    /// To configure the logger factory, please refer to https://learn.microsoft.com/en-us/dotnet/core/extensions/logging.
    /// </summary>
    public static class MqLogManager
    {
        private static ILoggerFactory _loggerFactory;

        private const string FileLogLevelKey = "rocketmq_log_level";
        private const string FileLogLevel = "Info";

        private const string FileLogRootKey = "rocketmq_log_root";

        private const string FileMaxIndexKey = "rocketmq_log_file_maxIndex";
        private const string FileMaxIndex = "10";

        static MqLogManager()
        {
            _loggerFactory = BuildDefaultLoggerFactory();
        }

        public static ILogger<T> CreateLogger<T>()
        {
            return _loggerFactory.CreateLogger<T>();
        }

        public static ILogger CreateLogger(string categoryName)
        {
            return _loggerFactory.CreateLogger(categoryName);
        }

        public static void UseLoggerFactory(ILoggerFactory loggerFactory)
        {
            _loggerFactory = loggerFactory ?? throw new ArgumentNullException(nameof(loggerFactory));
        }

        // Build default logger factory with NLog.
        // Configure component logging, please refer to https://github.com/NLog/NLog/wiki/Configure-component-logging
        private static ILoggerFactory BuildDefaultLoggerFactory()
        {
            var fileLogLevel = Environment.GetEnvironmentVariable(FileLogLevelKey) ?? FileLogLevel;
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

            var loggerFactory = LoggerFactory.Create(builder => builder.AddNLog(config));

            return loggerFactory;
        }
    }
}