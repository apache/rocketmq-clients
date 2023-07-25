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

from rocketmq.log import logger
from rocketmq.message import Message
from rocketmq.protocol.definition_pb2 import Code as ProtoCode
from rocketmq.protocol.definition_pb2 import Message as ProtoMessage
from rocketmq.protocol.definition_pb2 import Status as ProtoStatus
from rocketmq.protocol.service_pb2 import \
    ReceiveMessageRequest as ProtoReceiveMessageRequest


class RocketMQException(Exception):
    def __init__(self, status_code, request_id, status_message):
        self.status_code = status_code
        self.request_id = request_id
        self.status_message = status_message

    def __str__(self):
        return f"{self.__class__.__name__}: code={self.status_code}, requestId={self.request_id}, message={self.status_message}"


class BadRequestException(RocketMQException):
    pass


class UnauthorizedException(RocketMQException):
    pass


class PaymentRequiredException(RocketMQException):
    pass


class ForbiddenException(RocketMQException):
    pass


class NotFoundException(RocketMQException):
    pass


class PayloadTooLargeException(RocketMQException):
    pass


class TooManyRequestsException(RocketMQException):
    pass


class RequestHeaderFieldsTooLargeException(RocketMQException):
    pass


class InternalErrorException(RocketMQException):
    pass


class ProxyTimeoutException(RocketMQException):
    pass


class UnsupportedException(RocketMQException):
    pass


class StatusChecker:
    @staticmethod
    def check(status: ProtoStatus, request: Message, request_id: str):
        """Check the status of a request and raise an exception if necessary.

        :param status: A ProtoStatus object that contains the status code and message.
        :param request: The request message object.
        :param request_id: The ID of the request.
        :raise BadRequestException: If the status code indicates a bad request.
        :raise UnauthorizedException: If the status code indicates an unauthorized request.
        :raise PaymentRequiredException: If the status code indicates payment is required.
        :raise ForbiddenException: If the status code indicates a forbidden request.
        :raise NotFoundException: If the status code indicates a resource is not found.
        :raise PayloadTooLargeException: If the status code indicates the request payload is too large.
        :raise TooManyRequestsException: If the status code indicates too many requests.
        :raise RequestHeaderFieldsTooLargeException: If the status code indicates the request headers are too large.
        :raise InternalErrorException: If the status code indicates an internal error.
        :raise ProxyTimeoutException: If the status code indicates a proxy timeout.
        :raise UnsupportedException: If the status code indicates an unsupported operation.
        """
        status_code = status.code
        status_message = status.message

        if status_code in [ProtoCode.OK, ProtoCode.MULTIPLE_RESULTS]:
            return
        elif status_code in [
            ProtoCode.BAD_REQUEST,
            ProtoCode.ILLEGAL_ACCESS_POINT,
            ProtoCode.ILLEGAL_TOPIC,
            ProtoCode.ILLEGAL_CONSUMER_GROUP,
            ProtoCode.ILLEGAL_MESSAGE_TAG,
            ProtoCode.ILLEGAL_MESSAGE_KEY,
            ProtoCode.ILLEGAL_MESSAGE_GROUP,
            ProtoCode.ILLEGAL_MESSAGE_PROPERTY_KEY,
            ProtoCode.INVALID_TRANSACTION_ID,
            ProtoCode.ILLEGAL_MESSAGE_ID,
            ProtoCode.ILLEGAL_FILTER_EXPRESSION,
            ProtoCode.ILLEGAL_INVISIBLE_TIME,
            ProtoCode.ILLEGAL_DELIVERY_TIME,
            ProtoCode.INVALID_RECEIPT_HANDLE,
            ProtoCode.MESSAGE_PROPERTY_CONFLICT_WITH_TYPE,
            ProtoCode.UNRECOGNIZED_CLIENT_TYPE,
            ProtoCode.MESSAGE_CORRUPTED,
            ProtoCode.CLIENT_ID_REQUIRED,
            ProtoCode.ILLEGAL_POLLING_TIME,
        ]:
            raise BadRequestException(status_code, request_id, status_message)
        elif status_code == ProtoCode.UNAUTHORIZED:
            raise UnauthorizedException(status_code, request_id, status_message)
        elif status_code == ProtoCode.PAYMENT_REQUIRED:
            raise PaymentRequiredException(status_code, request_id, status_message)
        elif status_code == ProtoCode.FORBIDDEN:
            raise ForbiddenException(status_code, request_id, status_message)
        elif status_code == ProtoCode.MESSAGE_NOT_FOUND:
            if isinstance(request, ProtoReceiveMessageRequest):
                return
            else:
                # Fall through on purpose.
                status_code = ProtoCode.NOT_FOUND
        if status_code in [
            ProtoCode.NOT_FOUND,
            ProtoCode.TOPIC_NOT_FOUND,
            ProtoCode.CONSUMER_GROUP_NOT_FOUND,
        ]:
            raise NotFoundException(status_code, request_id, status_message)
        elif status_code in [
            ProtoCode.PAYLOAD_TOO_LARGE,
            ProtoCode.MESSAGE_BODY_TOO_LARGE,
        ]:
            raise PayloadTooLargeException(status_code, request_id, status_message)
        elif status_code == ProtoCode.TOO_MANY_REQUESTS:
            raise TooManyRequestsException(status_code, request_id, status_message)
        elif status_code in [
            ProtoCode.REQUEST_HEADER_FIELDS_TOO_LARGE,
            ProtoCode.MESSAGE_PROPERTIES_TOO_LARGE,
        ]:
            raise RequestHeaderFieldsTooLargeException(status_code, request_id, status_message)
        elif status_code in [
            ProtoCode.INTERNAL_ERROR,
            ProtoCode.INTERNAL_SERVER_ERROR,
            ProtoCode.HA_NOT_AVAILABLE,
        ]:
            raise InternalErrorException(status_code, request_id, status_message)
        elif status_code in [
            ProtoCode.PROXY_TIMEOUT,
            ProtoCode.MASTER_PERSISTENCE_TIMEOUT,
            ProtoCode.SLAVE_PERSISTENCE_TIMEOUT,
        ]:
            raise ProxyTimeoutException(status_code, request_id, status_message)
        elif status_code in [
            ProtoCode.UNSUPPORTED,
            ProtoCode.VERSION_UNSUPPORTED,
            ProtoCode.VERIFY_FIFO_MESSAGE_UNSUPPORTED,
        ]:
            raise UnsupportedException(status_code, request_id, status_message)
        else:
            logger.warning(f"Unrecognized status code={status_code}, requestId={request_id}, statusMessage={status_message}")
            raise UnsupportedException(status_code, request_id, status_message)


def main():
    # 创建一个表示'OK'状态的ProtoStatus
    status_ok = ProtoStatus()
    status_ok.code = ProtoCode.OK
    status_ok.message = "Everything is OK"

    # 创建一个表示'BadRequest'状态的ProtoStatus
    status_bad_request = ProtoStatus()
    status_bad_request.code = ProtoCode.BAD_REQUEST
    status_bad_request.message = "Bad request"

    # 创建一个表示'Unauthorized'状态的ProtoStatus
    status_unauthorized = ProtoStatus()
    status_unauthorized.code = ProtoCode.UNAUTHORIZED
    status_unauthorized.message = "Unauthorized"

    request = ProtoMessage()

    # 进行一些测试
    StatusChecker.check(status_ok, request, "request1")  # 不应抛出异常

    try:
        StatusChecker.check(status_bad_request, request, "request2")
    except BadRequestException as e:
        logger.error(f"Caught expected exception: {e}")

    try:
        StatusChecker.check(status_unauthorized, request, "request3")
    except UnauthorizedException as e:
        logger.error(f"Caught expected exception: {e}")


if __name__ == "__main__":
    main()
