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

    private static function resolveExceptionClass($code)
    {
        if (in_array($code, self::$badRequestCodes)) {
            return BadRequestException::class;
        }

        switch ($code) {
            case 40100:
                return UnauthorizedException::class;
            case 40200:
                return PaymentRequiredException::class;
            case 40300:
                return ForbiddenException::class;
            case 40400:
            case 40401:
            case 40402:
                return NotFoundException::class;
            case 41300:
            case 41301:
                return PayloadTooLargeException::class;
            case 41400:
            case 41401:
                return PayloadEmptyException::class;
            case 42900:
                return TooManyRequestsException::class;
            case 40901:
                return LiteTopicQuotaExceededException::class;
            case 40902:
                return LiteSubscriptionQuotaExceededException::class;
            case 43100:
            case 43101:
                return RequestHeaderFieldsTooLargeException::class;
            case 50000:
            case 50001:
            case 50002:
                return InternalErrorException::class;
            case 50400:
            case 50401:
            case 50402:
                return ProxyTimeoutException::class;
            case 50100:
            case 50101:
            case 50102:
                return UnsupportedException::class;
            default:
                if ($code >= 40000 && $code < 50000) {
                    return BadRequestException::class;
                }
                return InternalErrorException::class;
        }
    }

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
