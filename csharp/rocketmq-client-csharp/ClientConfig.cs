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
using System.Collections.Generic;
using rmq = Apache.Rocketmq.V2;

namespace Org.Apache.Rocketmq
{

    public class ClientConfig : IClientConfig
    {

        public ClientConfig()
        {
            var hostName = System.Net.Dns.GetHostName();
            var pid = System.Diagnostics.Process.GetCurrentProcess().Id;
            this.clientId_ = string.Format("{0}@{1}#{2}", hostName, pid, instanceName_);
            this._requestTimeout = TimeSpan.FromSeconds(3);
            this.longPollingIoTimeout_ = TimeSpan.FromSeconds(30);
            this.client_type_ = rmq::ClientType.Unspecified;
            this.access_point_ = new rmq::Endpoints();
            this.back_off_policy_ = new rmq::RetryPolicy();
            this._publishing = new Publishing();
        }

        public string region()
        {
            return _region;
        }
        public string Region
        {
            set { _region = value; }
        }

        public string serviceName()
        {
            return _serviceName;
        }
        public string ServiceName
        {
            set { _serviceName = value; }
        }

        public string resourceNamespace()
        {
            return _resourceNamespace;
        }
        public string ResourceNamespace
        {
            get { return _resourceNamespace; }
            set { _resourceNamespace = value; }
        }

        public ICredentialsProvider credentialsProvider()
        {
            return credentialsProvider_;
        }

        public ICredentialsProvider CredentialsProvider
        {
            set { credentialsProvider_ = value; }
        }

        public string tenantId()
        {
            return _tenantId;
        }
        public string TenantId
        {
            set { _tenantId = value; }
        }

        public TimeSpan RequestTimeout
        {
            get
            {
                return _requestTimeout;
            }
            set
            {
                _requestTimeout = value;
            }
        }

        public TimeSpan getLongPollingTimeout()
        {
            return longPollingIoTimeout_;
        }
        public TimeSpan LongPollingTimeout
        {
            set { longPollingIoTimeout_ = value; }
        }

        public string getGroupName()
        {
            return groupName_;
        }
        public string GroupName
        {
            set { groupName_ = value; }
        }

        public string clientId()
        {
            return clientId_;
        }

        public bool isTracingEnabled()
        {
            return tracingEnabled_;
        }
        public bool TracingEnabled
        {
            set { tracingEnabled_ = value; }
        }

        public void setInstanceName(string instanceName)
        {
            this.instanceName_ = instanceName;
        }

        private string _region = "cn-hangzhou";
        private string _serviceName = "ONS";

        protected string _resourceNamespace;

        private ICredentialsProvider credentialsProvider_;

        private string _tenantId;

        private TimeSpan _requestTimeout;

        private TimeSpan longPollingIoTimeout_;

        private string groupName_;

        private string clientId_;

        private bool tracingEnabled_ = false;

        private string instanceName_ = "default";

        private rmq::ClientType client_type_;
        public rmq::ClientType ClientType
        {
            get { return client_type_; }
            set { client_type_ = value; }
        }


        private rmq::Endpoints access_point_;

        public rmq::AddressScheme AccessPointScheme
        {
            get { return access_point_.Scheme; }
            set { access_point_.Scheme = value; }
        }

        public List<rmq::Address> AccessPointEndpoints
        {
            get
            {
                List<rmq::Address> addresses = new List<rmq::Address>();
                foreach (var item in access_point_.Addresses)
                {
                    addresses.Add(item);
                }
                return addresses;
            }

            set
            {
                access_point_.Addresses.Clear();
                foreach (var item in value)
                {
                    access_point_.Addresses.Add(item);
                }
            }
        }

        private rmq::RetryPolicy back_off_policy_;

        private Publishing _publishing;
        public Publishing Publishing
        {
            get { return _publishing; }
        }

    }

}
