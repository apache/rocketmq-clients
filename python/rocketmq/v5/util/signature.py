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

from binascii import hexlify
from datetime import datetime
from hashlib import sha1
from hmac import new
from uuid import uuid4

from rocketmq.v5.util import ClientId, Misc


class Signature:

    @staticmethod
    def metadata(config, client_id: ClientId):
        now = datetime.now()
        formatted_date_time = now.strftime("%Y%m%dT%H%M%SZ")
        request_id = str(uuid4())
        sign = Signature.sign(config.credentials.sk, formatted_date_time)
        authorization = (
            "MQv2-HMAC-SHA1"
            + " "
            + "Credential"
            + "="
            + config.credentials.ak
            + ", "
            + "SignedHeaders"
            + "="
            + "x-mq-date-time"
            + ", "
            + "Signature"
            + "="
            + sign
        )
        metadata = [
            ("x-mq-language", "PYTHON"),
            ("x-mq-protocol", "GRPC_V2"),
            ("x-mq-client-version", Misc.sdk_version()),
            ("x-mq-date-time", formatted_date_time),
            ("x-mq-request-id", request_id),
            ("x-mq-client-id", client_id),
            ("x-mq-namespace", config.namespace),
            ("authorization", authorization),
        ]
        return metadata

    @staticmethod
    def sign(access_secret, date_time):
        signing_key = access_secret.encode("utf-8")
        mac = new(signing_key, date_time.encode("utf-8"), sha1)
        return hexlify(mac.digest()).decode("utf-8")
