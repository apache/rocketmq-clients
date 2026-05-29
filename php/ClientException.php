<?php
/**
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

namespace Apache\Rocketmq;

class ClientException extends \Exception
{
    protected $code;

    /**
     * Construct a client exception with a status code and optional message.
     *
     * @param int $code Error status code
     * @param string $message Optional error message
     */
    public function __construct(int $code, string $message = '')
    {
        $this->code = $code;
        parent::__construct($message ?: "Client error with code: {$code}", $code);
    }

    /**
     * Get the error status code.
     *
     * @return int The error status code
     */
    public function getStatusCode(): int
    {
        return $this->code;
    }
}

class BadRequestException extends ClientException {}
class UnauthorizedException extends ClientException {}
class PaymentRequiredException extends ClientException {}
class ForbiddenException extends ClientException {}
class NotFoundException extends ClientException {}
class PayloadTooLargeException extends ClientException {}
class PayloadEmptyException extends ClientException {}
class TooManyRequestsException extends ClientException {}
class LiteTopicQuotaExceededException extends ClientException {}
class LiteSubscriptionQuotaExceededException extends ClientException {}
class RequestHeaderFieldsTooLargeException extends ClientException {}
class InternalErrorException extends ClientException {}
class ProxyTimeoutException extends ClientException {}
class UnsupportedException extends ClientException {}
