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

namespace Org.Apache.Rocketmq
{

    /**
     * File-based credentials provider that reads JSON configurations from ${HOME}/.rocketmq/config
     * A sample config content is as follows:
     * {"AccessKey": "key", "AccessSecret": "secret"}
     */
    public class ConfigFileCredentialsProvider : ICredentialsProvider
    {

        public ConfigFileCredentialsProvider()
        {
            var home = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);
            string configFileRelativePath = "/.rocketmq/config";
            if (!File.Exists(home + configFileRelativePath))
            {
                return;
            }

            try
            {
                using (var reader = new StreamReader(home + configFileRelativePath))
                {
                    string json = reader.ReadToEnd();
                    var kv = JsonSerializer.Deserialize<Dictionary<string, string>>(json);
                    accessKey = kv["AccessKey"];
                    accessSecret = kv["AccessSecret"];
                    valid = true;
                }
            }
            catch (IOException)
            {
            }
        }

        public Credentials getCredentials()
        {
            if (!valid)
            {
                return null;
            }

            return new Credentials(accessKey, accessSecret);
        }

        private string accessKey;
        private string accessSecret;

        private bool valid = false;
    }
}