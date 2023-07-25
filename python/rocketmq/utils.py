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

import hashlib
import hmac

master_broker_id = 0


def number_to_base(number, base):
    alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
    if number == 0:
        return alphabet[0]

    result = []
    while number:
        number, remainder = divmod(number, base)
        result.append(alphabet[remainder])

    return "".join(reversed(result))


def sign(access_secret: str, datetime: str) -> str:
    digester = hmac.new(
        bytes(access_secret, encoding="UTF-8"),
        bytes(datetime, encoding="UTF-8"),
        hashlib.sha1,
    )
    return digester.hexdigest().upper()


def get_positive_mod(k: int, n: int):
    result = k % n
    return result + n if result < 0 else result
