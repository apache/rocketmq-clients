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

from rocketmq.rpc_client import Endpoints
from rocketmq.session_credentials import SessionCredentialsProvider


class ClientConfig:
    """Client configuration class which holds the settings for a client.
    The settings include endpoint configurations, session credential provider and SSL settings.
    An instance of this class is used to setup the client with necessary configurations.
    """

    def __init__(
        self,
        endpoints: Endpoints,
        session_credentials_provider: SessionCredentialsProvider,
        ssl_enabled: bool,
    ):
        #: The endpoints for the client to connect to.
        self.__endpoints = endpoints

        #: The session credentials provider to authenticate the client.
        self.__session_credentials_provider = session_credentials_provider

        #: A flag indicating if SSL is enabled for the client.
        self.__ssl_enabled = ssl_enabled

        #: The request timeout for the client in seconds.
        self.request_timeout = 10

    @property
    def session_credentials_provider(self) -> SessionCredentialsProvider:
        """The session credentials provider for the client.

        :return: the session credentials provider
        """
        return self.__session_credentials_provider

    @property
    def endpoints(self) -> Endpoints:
        """The endpoints for the client to connect to.

        :return: the endpoints
        """
        return self.__endpoints

    @property
    def ssl_enabled(self) -> bool:
        """A flag indicating if SSL is enabled for the client.

        :return: True if SSL is enabled, False otherwise
        """
        return self.__ssl_enabled
