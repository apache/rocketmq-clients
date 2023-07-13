# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


class SessionCredentials:
    def __init__(self, access_key=None, access_secret=None, security_token=None):
        if access_key is None:
            raise ValueError("accessKey should not be None")
        if access_secret is None:
            raise ValueError("accessSecret should not be None")

        self.access_key = access_key
        self.access_secret = access_secret
        self.security_token = security_token


class SessionCredentialsProvider:
    def __init__(self, credentials):
        if not isinstance(credentials, SessionCredentials):
            raise ValueError("credentials should be an instance of SessionCredentials")
        self.credentials = credentials

    def get_credentials(self):
        return self.credentials
