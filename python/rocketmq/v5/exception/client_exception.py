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


class ClientException(Exception):

    def __init__(self, message, code=None):
        super().__init__(message)
        self.__code = code

    def __str__(self):
        if self.__code is not None:
            return f"{self.__code}, {super().__str__()}"
        else:
            return f"{super().__str__()}"

    @property
    def code(self):
        return self.__code


class BadRequestException(ClientException):

    def __init__(self, message, code):
        super().__init__(message, code)


class UnauthorizedException(ClientException):

    def __init__(self, message, code):
        super().__init__(message, code)


class PaymentRequiredException(ClientException):

    def __init__(self, message, code):
        super().__init__(message, code)


class ForbiddenException(ClientException):

    def __init__(self, message, code):
        super().__init__(message, code)


class NotFoundException(ClientException):

    def __init__(self, message, code):
        super().__init__(message, code)


class PayloadTooLargeException(ClientException):

    def __init__(self, message, code):
        super().__init__(message, code)


class TooManyRequestsException(ClientException):

    def __init__(self, message, code):
        super().__init__(message, code)


class RequestHeaderFieldsTooLargeException(ClientException):

    def __init__(self, message, code):
        super().__init__(message, code)


class InternalErrorException(ClientException):

    def __init__(self, message, code):
        super().__init__(message, code)


class ProxyTimeoutException(ClientException):

    def __init__(self, message, code):
        super().__init__(message, code)


class UnsupportedException(ClientException):

    def __init__(self, message, code=None):
        super().__init__(message, code)


class IllegalArgumentException(ClientException):

    def __init__(self, message):
        super().__init__(message)


class IllegalStateException(ClientException):

    def __init__(self, message):
        super().__init__(message)
