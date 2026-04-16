<?php
declare(strict_types=1);
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

namespace Apache\Rocketmq\Exception;

use Apache\Rocketmq\V2\Code;
use Apache\Rocketmq\V2\Status;
use Apache\Rocketmq\Logger;

/**
 * Status Checker - Validates server response status and throws appropriate exceptions
 * 
 * Reference: Java StatusChecker
 */
class StatusChecker {
    
    /**
     * Check status and throw exception if needed
     * 
     * @param Status $status Server response status
     * @param string|null $requestId Request ID from RPC context
     * @param bool $isReceiveMessage Whether this is a receive message request
     * @return void
     * @throws ClientException Appropriate exception based on status code
     */
    public static function check(Status $status, ?string $requestId = null, bool $isReceiveMessage = false): void {
        $code = $status->getCode();
        $codeNumber = $code->getValue(); // Get enum value
        $statusMessage = $status->getMessage() ?? '';
        
        switch ($codeNumber) {
            case Code::OK:
            case Code::MULTIPLE_RESULTS:
                return;
                
            case Code::BAD_REQUEST:
            case Code::ILLEGAL_ACCESS_POINT:
            case Code::ILLEGAL_TOPIC:
            case Code::ILLEGAL_CONSUMER_GROUP:
            case Code::ILLEGAL_MESSAGE_TAG:
            case Code::ILLEGAL_MESSAGE_KEY:
            case Code::ILLEGAL_MESSAGE_GROUP:
            case Code::ILLEGAL_LITE_TOPIC:
            case Code::ILLEGAL_MESSAGE_PROPERTY_KEY:
            case Code::INVALID_TRANSACTION_ID:
            case Code::ILLEGAL_MESSAGE_ID:
            case Code::ILLEGAL_FILTER_EXPRESSION:
            case Code::ILLEGAL_INVISIBLE_TIME:
            case Code::ILLEGAL_DELIVERY_TIME:
            case Code::INVALID_RECEIPT_HANDLE:
            case Code::MESSAGE_PROPERTY_CONFLICT_WITH_TYPE:
            case Code::UNRECOGNIZED_CLIENT_TYPE:
            case Code::MESSAGE_CORRUPTED:
            case Code::CLIENT_ID_REQUIRED:
            case Code::ILLEGAL_POLLING_TIME:
                throw new BadRequestException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::UNAUTHORIZED:
                throw new UnauthorizedException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::PAYMENT_REQUIRED:
                throw new PaymentRequiredException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::FORBIDDEN:
                throw new ForbiddenException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::MESSAGE_NOT_FOUND:
                // For receive message request, MESSAGE_NOT_FOUND is normal (no messages available)
                if ($isReceiveMessage) {
                    return;
                }
                // Fall through for other cases
                
            case Code::NOT_FOUND:
            case Code::TOPIC_NOT_FOUND:
            case Code::CONSUMER_GROUP_NOT_FOUND:
                throw new NotFoundException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::PAYLOAD_TOO_LARGE:
            case Code::MESSAGE_BODY_TOO_LARGE:
                throw new PayloadTooLargeException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::MESSAGE_BODY_EMPTY:
                throw new PayloadEmptyException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::TOO_MANY_REQUESTS:
                throw new TooManyRequestsException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::LITE_TOPIC_QUOTA_EXCEEDED:
                throw new LiteTopicQuotaExceededException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::LITE_SUBSCRIPTION_QUOTA_EXCEEDED:
                throw new LiteSubscriptionQuotaExceededException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::REQUEST_HEADER_FIELDS_TOO_LARGE:
            case Code::MESSAGE_PROPERTIES_TOO_LARGE:
                throw new RequestHeaderFieldsTooLargeException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::INTERNAL_ERROR:
            case Code::INTERNAL_SERVER_ERROR:
            case Code::HA_NOT_AVAILABLE:
                throw new InternalErrorException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::PROXY_TIMEOUT:
            case Code::MASTER_PERSISTENCE_TIMEOUT:
            case Code::SLAVE_PERSISTENCE_TIMEOUT:
                throw new ProxyTimeoutException($codeNumber, $requestId ?? '', $statusMessage);
                
            case Code::UNSUPPORTED:
            case Code::VERSION_UNSUPPORTED:
            case Code::VERIFY_FIFO_MESSAGE_UNSUPPORTED:
                throw new UnsupportedException($codeNumber, $requestId ?? '', $statusMessage);
                
            default:
                Logger::warning("Unrecognized status code={}, requestId={}, statusMessage={}", [
                    $codeNumber,
                    $requestId ?? 'unknown',
                    $statusMessage
                ]);
                throw new UnsupportedException($codeNumber, $requestId ?? '', $statusMessage);
        }
    }
}
