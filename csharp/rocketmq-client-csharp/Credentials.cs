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

namespace Org.Apache.Rocketmq
{
    public class Credentials
    {

        public Credentials(string accessKey, string accessSecret)
        {
            this.accessKey = accessKey;
            this.accessSecret = accessSecret;
        }

        public Credentials(string accessKey, string accessSecret, string sessionToken, DateTime expirationInstant)
        {
            this.accessKey = accessKey;
            this.accessSecret = accessSecret;
            this.sessionToken = sessionToken;
            this.expirationInstant = expirationInstant;
        }

        public bool empty()
        {
            return String.IsNullOrEmpty(accessKey) || String.IsNullOrEmpty(accessSecret);
        }

        public bool expired()
        {
            if (DateTime.MinValue == expirationInstant)
            {
                return false;
            }

            return DateTime.Now > expirationInstant;
        }

        private string accessKey;
        public string AccessKey
        {
            get { return accessKey; }
        }

        private string accessSecret;
        public string AccessSecret
        {
            get { return accessSecret; }
        }

        private string sessionToken;
        public string SessionToken
        {
            get { return sessionToken; }
        }

        private DateTime expirationInstant = DateTime.MinValue;

    }
}