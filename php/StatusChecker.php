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

class StatusChecker
{
    private static $badRequestCodes = [
        40000, 40001, 40002, 40004, 40005, 40006,
    ];

    private static $statusMessageMap = [
        'BAD_REQUEST' => 1,
        'ILLEGAL_TOPIC' => 1,
        'ILLEGAL_CONSUMER_GROUP' => 1,
        'ILLEGAL_MESSAGE_TAG' => 1,
        'ILLEGAL_MESSAGE_KEY' => 1,
        'ILLEGAL_MESSAGE_GROUP' => 1,
        'ILLEGAL_MESSAGE_PROPERTY_KEY' => 1,
        'INVALID_TRANSACTION_ID' => 1,
        'MESSAGE_CORRUPTED' => 1,
        'ILLEGAL_FILTER_EXPRESSION' => 1,
        'ILLEGAL_FILTER_SQL92_EXPRESSION' => 1,
        'INVALID_RECEIPT_HANDLE' => 1,
        'WRONG_ORGANIZATION' => 1,
        'ILLEGAL_LITE_TOPIC' => 1,
        'ILLEGAL_GLOBAL_BID' => 1,

        'UNAUTHORIZED' => 2,

        'PAYMENT_REQUIRED' => 3,

        'FORBIDDEN' => 4,
        'FORBIDDEN_REUSE' => 4,

        'NOT_FOUND' => 5,
        'TOPIC_NOT_FOUND' => 5,
        'CONSUMER_GROUP_NOT_FOUND' => 5,

        'PAYLOAD_TOO_LARGE' => 6,
        'MESSAGE_BODY_TOO_LARGE' => 6,

        'PAYLOAD_EMPTY' => 7,
        'MESSAGE_BODY_EMPTY' => 7,

        'TOO_MANY_REQUESTS' => 8,

        'LITE_TOPIC_QUOTA_EXCEEDED' => 9,
        'LITE_SUBSCRIPTION_QUOTA_EXCEEDED' => 10,

        'REQUEST_HEADER_FIELDS_TOO_LARGE' => 11,
        'MESSAGE_PROPERTIES_TOO_LARGE' => 11,

        'INTERNAL_ERROR' => 12,
        'INTERNAL_SERVER_ERROR' => 12,
        'HA_NOT_AVAILABLE' => 12,

        'PROXY_TIMEOUT' => 13,
        'MASTER_PERSISTENCE_TIMEOUT' => 13,
        'SLAVE_PERSISTENCE_TIMEOUT' => 13,

        'UNSUPPORTED' => 14,
        'VERSION_UNSUPPORTED' => 14,
        'VERIFY_FIFO_MESSAGE_UNSUPPORTED' => 14,
    ];

    /**
     * Check gRPC status and throw appropriate exception on failure.
     *
     * @param \Google\Rpc\Status|null $status gRPC status object
     * @param string $detailMessage Optional additional detail message
     * @return void
     * @throws BadRequestException|UnauthorizedException|ForbiddenException|NotFoundException|InternalErrorException|TooManyRequestsException|\Exception
     */
    public static function check($status, $detailMessage = '')
    {
        if ($status === null) {
            return;
        }

        $code = $status->getCode();
        if ($code === 20000) {
            return;
        }

        $message = $status->getMessage();
        if (!empty($detailMessage)) {
            $message = $message . '; detail: ' . $detailMessage;
        }

        $exceptionClass = self::resolveExceptionClass($code);
        throw new $exceptionClass($code, $message);
    }

    /**
     * Resolve the exception class name for a given status code.
     *
     * @param int $code gRPC status code
     * @return string Fully qualified exception class name
     */
    private static function resolveExceptionClass($code)
    {
        if (in_array($code, self::$badRequestCodes)) {
            return BadRequestException::class;
        }

        return match ($code) {
            40100 => UnauthorizedException::class,
            40200 => PaymentRequiredException::class,
            40300 => ForbiddenException::class,
            40400, 40401, 40402 => NotFoundException::class,
            41300, 41301 => PayloadTooLargeException::class,
            41400, 41401 => PayloadEmptyException::class,
            42900 => TooManyRequestsException::class,
            40901 => LiteTopicQuotaExceededException::class,
            40902 => LiteSubscriptionQuotaExceededException::class,
            43100, 43101 => RequestHeaderFieldsTooLargeException::class,
            50000, 50001, 50002 => InternalErrorException::class,
            50400, 50401, 50402 => ProxyTimeoutException::class,
            50100, 50101, 50102 => UnsupportedException::class,
            default => ($code >= 40000 && $code < 50000)
                ? BadRequestException::class
                : InternalErrorException::class,
        };
    }

    /**
     * Map a status message string to a numeric code.
     *
     * @param string $statusMessage Human-readable status message
     * @return int Numeric status code, defaults to 40000
     */
    public static function codeFromStatusMessage($statusMessage)
    {
        $upper = strtoupper(trim($statusMessage));
        foreach (self::$statusMessageMap as $key => $code) {
            if (strpos($upper, $key) !== false) {
                return $code;
            }
        }
        return 40000;
    }
}
