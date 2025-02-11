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

from rocketmq.grpc_protocol import Code, Status
from rocketmq.v5.exception import (BadRequestException, ForbiddenException,
                                   InternalErrorException, NotFoundException,
                                   PayloadTooLargeException,
                                   PaymentRequiredException,
                                   ProxyTimeoutException,
                                   RequestHeaderFieldsTooLargeException,
                                   TooManyRequestsException,
                                   UnauthorizedException, UnsupportedException)
from rocketmq.v5.log import logger


class MessagingResultChecker:

    @staticmethod
    def check(status: Status):
        code = status.code
        message = status.message

        if code == Code.OK or code == Code.MULTIPLE_RESULTS:
            return
        elif (
            code == Code.BAD_REQUEST
            or code == Code.ILLEGAL_ACCESS_POINT
            or code == Code.ILLEGAL_TOPIC
            or code == Code.ILLEGAL_CONSUMER_GROUP
            or code == Code.ILLEGAL_MESSAGE_TAG
            or code == Code.ILLEGAL_MESSAGE_KEY
            or code == Code.ILLEGAL_MESSAGE_GROUP
            or code == Code.ILLEGAL_MESSAGE_PROPERTY_KEY
            or code == Code.INVALID_TRANSACTION_ID
            or code == Code.ILLEGAL_MESSAGE_ID
            or code == Code.ILLEGAL_FILTER_EXPRESSION
            or code == Code.ILLEGAL_INVISIBLE_TIME
            or code == Code.ILLEGAL_DELIVERY_TIME
            or code == Code.INVALID_RECEIPT_HANDLE
            or code == Code.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE
            or code == Code.UNRECOGNIZED_CLIENT_TYPE
            or code == Code.MESSAGE_CORRUPTED
            or code == Code.CLIENT_ID_REQUIRED
            or code == Code.ILLEGAL_POLLING_TIME
        ):
            raise BadRequestException(message, code)
        elif code == Code.UNAUTHORIZED:
            raise UnauthorizedException(message, code)
        elif code == Code.PAYMENT_REQUIRED:
            raise PaymentRequiredException(message, code)
        elif code == Code.FORBIDDEN:
            raise ForbiddenException(message, code)
        elif code == Code.MESSAGE_NOT_FOUND:
            return
        elif (
            code == Code.NOT_FOUND
            or code == Code.TOPIC_NOT_FOUND
            or code == Code.CONSUMER_GROUP_NOT_FOUND
        ):
            raise NotFoundException(message, code)
        elif code == Code.PAYLOAD_TOO_LARGE or code == Code.MESSAGE_BODY_TOO_LARGE:
            raise PayloadTooLargeException(message, code)
        elif code == Code.TOO_MANY_REQUESTS:
            raise TooManyRequestsException(message, code)
        elif (
            code == Code.REQUEST_HEADER_FIELDS_TOO_LARGE
            or code == Code.MESSAGE_PROPERTIES_TOO_LARGE
        ):
            raise RequestHeaderFieldsTooLargeException(message, code)
        elif (
            code == Code.INTERNAL_ERROR
            or code == Code.INTERNAL_SERVER_ERROR
            or code == Code.HA_NOT_AVAILABLE
        ):
            raise InternalErrorException(message, code)
        elif (
            code == Code.PROXY_TIMEOUT
            or code == Code.MASTER_PERSISTENCE_TIMEOUT
            or code == Code.SLAVE_PERSISTENCE_TIMEOUT
        ):
            raise ProxyTimeoutException(message, code)
        elif (
            code == Code.UNSUPPORTED
            or code == Code.VERSION_UNSUPPORTED
            or code == Code.VERIFY_FIFO_MESSAGE_UNSUPPORTED
        ):
            raise UnsupportedException(message, code)
        else:
            logger.warn(f"unrecognized status code:{code}, message:{message}")
            raise UnsupportedException(message, code)
