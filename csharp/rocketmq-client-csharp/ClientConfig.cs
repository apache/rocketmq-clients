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

namespace org.apache.rocketmq {

    public class ClientConfig : IClientConfig {

        public ClientConfig() {
            var hostName = System.Net.Dns.GetHostName();
            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
            this.clientId_ = string.Format("{0}@{1}#{2}", hostName, pid, instanceName_);
            this.ioTimeout_ = TimeSpan.FromSeconds(3);
            this.longPollingIoTimeout_ = TimeSpan.FromSeconds(15);
        }

        public string region() {
            return region_;
        }
        public string Region {
            set { region_ = value; }
        }

        public string serviceName() {
            return serviceName_;
        }
        public string ServiceName {
            set { serviceName_ = value; }
        }

        public string resourceNamespace() {
            return resourceNamespace_;
        }
        public string ResourceNamespace {
            set { resourceNamespace_ = value; }
        }

        public ICredentialsProvider credentialsProvider() {
            return credentialsProvider_;
        }
        
        public ICredentialsProvider CredentialsProvider {
            set { credentialsProvider_ = value; }
        }

        public string tenantId() {
            return tenantId_;
        }
        public string TenantId {
            set { tenantId_ = value; }
        }

        public TimeSpan getIoTimeout() {
            return ioTimeout_;
        }
        public TimeSpan IoTimeout {
            set { ioTimeout_ = value; }
        }

        public TimeSpan getLongPollingTimeout() {
            return longPollingIoTimeout_;
        }
        public TimeSpan LongPollingTimeout {
            set { longPollingIoTimeout_ = value; }
        }

        public string getGroupName() {
            return groupName_;
        }
        public string GroupName {
            set { groupName_ = value; }
        }

        public string clientId() {
            return clientId_;
        }

        public bool isTracingEnabled() {
            return tracingEnabled_;
        }
        public bool TracingEnabled {
            set { tracingEnabled_ = value; }
        }

        public void setInstanceName(string instanceName) {
            this.instanceName_ = instanceName;
        }

        private string region_ = "cn-hangzhou";
        private string serviceName_ = "ONS";

        protected string resourceNamespace_;

        private ICredentialsProvider credentialsProvider_;

        private string tenantId_;

        private TimeSpan ioTimeout_;

        private TimeSpan longPollingIoTimeout_;

        private string groupName_;

        private string clientId_;

        private bool tracingEnabled_ = false;

        private string instanceName_ = "default";
    }

}
