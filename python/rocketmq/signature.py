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

from rocketmq.client_config import ClientConfig
import uuid
import datetime
import importlib.metadata
from rocketmq.utils import sign


class Signature:
    __AUTHORIZATION_KEY = "authorization"

    __DATE_TIME_KEY = "x-mq-date-time"
    __DATE_TIME_FORMAT = "%Y%m%dT%H%M%SZ"

    __SESSION_TOKEN_KEY = "x-mq-session-token"
    __CLIENT_ID_KEY = "x-mq-client-id"
    __REQUEST_ID_KEY = "x-mq-request-id"
    __LANGUAGE_KEY = "x-mq-language"
    __CLIENT_VERSION_KEY = "x-mq-client-version"
    __PROTOCOL_VERSION = "x-mq-protocol"

    __ALGORITHM = "MQv2-HMAC-SHA1"
    __CREDENTIAL = "Credential"
    __SIGNED_HEADERS = "SignedHeaders"
    __SIGNATURE = "Signature"

    @staticmethod
    def sign(client_config: ClientConfig, client_id: str):
        date_time = datetime.datetime.now().strftime(Signature.__DATE_TIME_FORMAT)
        metadata = [
            (Signature.__LANGUAGE_KEY, "PYTHON"),
            (Signature.__PROTOCOL_VERSION, "v2"),
            (Signature.__CLIENT_VERSION_KEY, importlib.metadata.version("rocketmq")),
            (
                Signature.__DATE_TIME_KEY,
                date_time,
            ),
            (Signature.__REQUEST_ID_KEY, uuid.uuid4()),
            (Signature.__CLIENT_ID_KEY, client_id),
        ]
        if not client_config.session_credentials_provider:
            return metadata
        session_credentials = (
            client_config.session_credentials_provider.session_credentials()
        )
        if not session_credentials:
            return metadata
        if session_credentials.security_token:
            metadata.append(
                (Signature.__SESSION_TOKEN_KEY, session_credentials.security_token)
            )
        if (not session_credentials.access_key) or (
            not session_credentials.access_secret
        ):
            return metadata
        signature = sign(session_credentials.access_key, date_time)
        authorization = (
            Signature.__ALGORITHM
            + " "
            + Signature.__CREDENTIAL
            + "="
            + session_credentials.access_key
            + ", "
            + Signature.__SIGNED_HEADERS
            + "="
            + Signature.__DATE_TIME_KEY
            + ", "
            + Signature.__SIGNATURE
            + "="
            + signature
        )
        metadata.append((Signature.__AUTHORIZATION_KEY, authorization))
        return metadata
