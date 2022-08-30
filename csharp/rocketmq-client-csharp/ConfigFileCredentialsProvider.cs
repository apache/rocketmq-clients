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
using System.IO;
using System;
using System.Text.Json;
using System.Collections.Generic;
using NLog;

namespace Org.Apache.Rocketmq
{

    /**
     * File-based credentials provider that reads JSON configurations from ${HOME}/.rocketmq/config
     * A sample config content is as follows:
     * {"AccessKey": "key", "AccessSecret": "secret"}
     */
    public class ConfigFileCredentialsProvider : ICredentialsProvider
    {
        private static readonly Logger Logger = MqLogManager.Instance.GetCurrentClassLogger();

        public ConfigFileCredentialsProvider()
        {
            var configFilePath = DefaultConfigFilePath();

            if (!File.Exists(configFilePath))
            {
                Logger.Warn("Config file[{}] does not exist", configFilePath);
                return;
            }

            try
            {
                using var reader = new StreamReader(configFilePath);
                string json = reader.ReadToEnd();
                var kv = JsonSerializer.Deserialize<Dictionary<string, string>>(json);
                if (null == kv)
                {
                    Logger.Error($"Failed to parse JSON configuration: {json}");
                    return;
                }
                
                _accessKey = kv["AccessKey"];
                _accessSecret = kv["AccessSecret"];
                _valid = true;
            }
            catch (IOException e)
            {
                Logger.Error($"Failed to read cofig file. Cause: {e.Message}");
            }
        }

        public Credentials getCredentials()
        {
            if (!_valid)
            {
                return null;
            }

            return new Credentials(_accessKey, _accessSecret);
        }

        public static String DefaultConfigFilePath()
        {
            var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            string[] pathSegments = {home, ".rocketmq", "config"}; 
            return String.Join(Path.DirectorySeparatorChar, pathSegments);
        }

        private readonly string _accessKey;
        private readonly string _accessSecret;

        private readonly bool _valid;
    }
}