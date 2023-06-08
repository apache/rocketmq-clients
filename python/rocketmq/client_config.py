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


class ClientConfig:
    def __init__(self, endpoints: str, session_credentials_provider, ssl_enabled: bool):
        self.__endpoints = endpoints
        self.__session_credentials_provider = session_credentials_provider
        self.__ssl_enabled = ssl_enabled

    @property
    def session_credentials_provider(self):
        return self.__session_credentials_provider

    @property
    def endpoints(self):
        return self.__endpoints

    @property
    def ssl_enabled(self):
        return self.__ssl_enabled
